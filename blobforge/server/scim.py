"""SCIM 2.0 provisioning endpoints used by the infrastructure IdP."""

from __future__ import annotations

import json
import re
import secrets
import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import JSONResponse

from .database import Database, now_ms

SCIM = "application/scim+json"
USER_SCHEMA = "urn:ietf:params:scim:schemas:core:2.0:User"
GROUP_SCHEMA = "urn:ietf:params:scim:schemas:core:2.0:Group"
LIST_SCHEMA = "urn:ietf:params:scim:api:messages:2.0:ListResponse"
ERROR_SCHEMA = "urn:ietf:params:scim:api:messages:2.0:Error"
PATCH_SCHEMA = "urn:ietf:params:scim:api:messages:2.0:PatchOp"


def _response(value: Any, status: int = 200, headers: dict[str, str] | None = None) -> JSONResponse:
    return JSONResponse(value, status_code=status, headers=headers, media_type=SCIM)


def _meta(kind: str, identifier: str, created: int, updated: int, version: int) -> dict[str, Any]:
    return {"resourceType": kind, "created": created, "lastModified": updated,
            "location": f"/scim/v2/{kind}s/{identifier}", "version": f'W/"{version}"'}


def _user(row: Any) -> dict[str, Any]:
    return {"schemas": [USER_SCHEMA], "id": row["id"], "externalId": row["external_id"],
            "userName": row["user_name"], "displayName": row["display_name"],
            "active": bool(row["active"]), "emails": json.loads(row["emails_json"]),
            "meta": _meta("User", row["id"], row["created_at"], row["updated_at"], row["version"])}


def _group(db: Any, row: Any) -> dict[str, Any]:
    members = [{"value": value["user_id"], "$ref": f"/scim/v2/Users/{value['user_id']}"}
               for value in db.execute("SELECT user_id FROM scim_group_members WHERE group_id=? ORDER BY user_id", (row["id"],))]
    return {"schemas": [GROUP_SCHEMA], "id": row["id"], "externalId": row["external_id"],
            "displayName": row["display_name"], "members": members,
            "meta": _meta("Group", row["id"], row["created_at"], row["updated_at"], row["version"])}


def _filter_clause(value: str | None, allowed: set[str]) -> tuple[str, list[str]]:
    if not value:
        return "", []
    match = re.fullmatch(r'\s*([A-Za-z]+)\s+eq\s+"([^"\\]*)"\s*', value)
    if not match or match.group(1) not in allowed:
        raise HTTPException(400, "unsupported SCIM filter")
    columns = {"externalId": "external_id", "userName": "user_name", "displayName": "display_name"}
    return f" WHERE {columns[match.group(1)]}=?", [match.group(2)]


def create_scim_router(database: Database, bearer_token: str) -> APIRouter:
    async def authenticate(request: Request) -> None:
        supplied = request.headers.get("authorization", "")
        expected = f"Bearer {bearer_token}"
        if not bearer_token or not secrets.compare_digest(supplied, expected):
            raise HTTPException(401, "invalid SCIM bearer token")

    router = APIRouter(prefix="/scim/v2", dependencies=[Depends(authenticate)])

    @router.get("/ServiceProviderConfig")
    async def service_provider_config() -> JSONResponse:
        return _response({"schemas": ["urn:ietf:params:scim:schemas:core:2.0:ServiceProviderConfig"],
                          "patch": {"supported": True}, "bulk": {"supported": False},
                          "filter": {"supported": True, "maxResults": 200},
                          "changePassword": {"supported": False}, "sort": {"supported": False},
                          "etag": {"supported": True}, "authenticationSchemes": [{"type": "oauthbearertoken", "name": "Bearer token", "primary": True}]})

    @router.get("/ResourceTypes")
    async def resource_types() -> JSONResponse:
        resources = [{"schemas": ["urn:ietf:params:scim:schemas:core:2.0:ResourceType"], "id": kind,
                      "name": kind, "endpoint": f"/{kind}s", "schema": schema}
                     for kind, schema in (("User", USER_SCHEMA), ("Group", GROUP_SCHEMA))]
        return _response({"schemas": [LIST_SCHEMA], "totalResults": 2, "startIndex": 1, "itemsPerPage": 2, "Resources": resources})

    @router.get("/Schemas")
    async def schemas() -> JSONResponse:
        resources = [{"schemas": ["urn:ietf:params:scim:schemas:core:2.0:Schema"], "id": schema, "name": name, "attributes": []}
                     for name, schema in (("User", USER_SCHEMA), ("Group", GROUP_SCHEMA))]
        return _response({"schemas": [LIST_SCHEMA], "totalResults": 2, "startIndex": 1, "itemsPerPage": 2, "Resources": resources})

    @router.get("/Users")
    async def list_users(filter: str | None = None, startIndex: int = 1, count: int = 100) -> JSONResponse:
        clause, params = _filter_clause(filter, {"externalId", "userName"})
        with database.connect() as db:
            total = db.execute(f"SELECT COUNT(*) FROM scim_users{clause}", params).fetchone()[0]
            rows = list(db.execute(f"SELECT * FROM scim_users{clause} ORDER BY user_name LIMIT ? OFFSET ?", (*params, min(count, 200), max(0, startIndex - 1))))
        return _response({"schemas": [LIST_SCHEMA], "totalResults": total, "startIndex": startIndex,
                          "itemsPerPage": len(rows), "Resources": [_user(row) for row in rows]})

    @router.post("/Users")
    async def create_user(request: Request) -> JSONResponse:
        body = await request.json(); timestamp = now_ms(); identifier = str(uuid.uuid4())
        try:
            with database.transaction() as db:
                db.execute("""INSERT INTO scim_users(id,external_id,user_name,display_name,active,emails_json,raw_json,created_at,updated_at)
                    VALUES(?,?,?,?,?,?,?,?,?)""", (identifier, body.get("externalId"), str(body["userName"]), str(body.get("displayName") or ""),
                    int(body.get("active", True)), json.dumps(body.get("emails") or []), json.dumps(body), timestamp, timestamp))
                row = db.execute("SELECT * FROM scim_users WHERE id=?", (identifier,)).fetchone()
        except (KeyError, Exception) as exc:
            if isinstance(exc, KeyError): raise HTTPException(400, "userName is required")
            if "UNIQUE constraint" in str(exc): raise HTTPException(409, "SCIM user already exists")
            raise
        return _response(_user(row), 201, {"Location": f"/scim/v2/Users/{identifier}", "ETag": f'W/"{row["version"]}"'})

    @router.get("/Users/{identifier}")
    async def get_user(identifier: str) -> JSONResponse:
        with database.connect() as db: row = db.execute("SELECT * FROM scim_users WHERE id=?", (identifier,)).fetchone()
        if not row: raise HTTPException(404, "SCIM user not found")
        return _response(_user(row), headers={"ETag": f'W/"{row["version"]}"'})

    async def update_user(identifier: str, request: Request, patch: bool) -> JSONResponse:
        body = await request.json()
        with database.transaction() as db:
            row = db.execute("SELECT * FROM scim_users WHERE id=?", (identifier,)).fetchone()
            if not row: raise HTTPException(404, "SCIM user not found")
            values = _user(row)
            if patch:
                for operation in body.get("Operations", []):
                    op = str(operation.get("op", "replace")).lower(); path = operation.get("path"); value = operation.get("value")
                    changes = value if path is None and isinstance(value, dict) else {path: value}
                    for name, item in changes.items():
                        if name in {"userName", "displayName", "active", "emails", "externalId"}:
                            values[name] = None if op == "remove" else item
            else:
                values.update(body)
            db.execute("""UPDATE scim_users SET external_id=?,user_name=?,display_name=?,active=?,emails_json=?,raw_json=?,
                version=version+1,updated_at=? WHERE id=?""", (values.get("externalId"), values.get("userName") or row["user_name"],
                values.get("displayName") or "", int(values.get("active", True)), json.dumps(values.get("emails") or []),
                json.dumps(body), now_ms(), identifier))
            result = db.execute("SELECT * FROM scim_users WHERE id=?", (identifier,)).fetchone()
        return _response(_user(result), headers={"ETag": f'W/"{result["version"]}"'})

    @router.put("/Users/{identifier}")
    async def replace_user(identifier: str, request: Request) -> JSONResponse: return await update_user(identifier, request, False)

    @router.patch("/Users/{identifier}")
    async def patch_user(identifier: str, request: Request) -> JSONResponse: return await update_user(identifier, request, True)

    @router.delete("/Users/{identifier}", status_code=204)
    async def delete_user(identifier: str) -> Response:
        with database.transaction() as db: changed = db.execute("DELETE FROM scim_users WHERE id=?", (identifier,)).rowcount
        if not changed: raise HTTPException(404, "SCIM user not found")
        return Response(status_code=204)

    @router.get("/Groups")
    async def list_groups(filter: str | None = None, startIndex: int = 1, count: int = 100) -> JSONResponse:
        clause, params = _filter_clause(filter, {"externalId", "displayName"})
        with database.connect() as db:
            total = db.execute(f"SELECT COUNT(*) FROM scim_groups{clause}", params).fetchone()[0]
            rows = list(db.execute(f"SELECT * FROM scim_groups{clause} ORDER BY display_name LIMIT ? OFFSET ?", (*params, min(count, 200), max(0, startIndex - 1))))
            resources = [_group(db, row) for row in rows]
        return _response({"schemas": [LIST_SCHEMA], "totalResults": total, "startIndex": startIndex,
                          "itemsPerPage": len(rows), "Resources": resources})

    def set_members(db: Any, group_id: str, members: list[Any]) -> None:
        db.execute("DELETE FROM scim_group_members WHERE group_id=?", (group_id,))
        for member in members:
            value = str(member.get("value") if isinstance(member, dict) else member)
            if db.execute("SELECT 1 FROM scim_users WHERE id=?", (value,)).fetchone():
                db.execute("INSERT OR IGNORE INTO scim_group_members VALUES(?,?)", (group_id, value))

    @router.post("/Groups")
    async def create_group(request: Request) -> JSONResponse:
        body = await request.json(); timestamp = now_ms(); identifier = str(uuid.uuid4())
        try:
            with database.transaction() as db:
                db.execute("INSERT INTO scim_groups(id,external_id,display_name,raw_json,created_at,updated_at) VALUES(?,?,?,?,?,?)",
                           (identifier, body.get("externalId"), str(body["displayName"]), json.dumps(body), timestamp, timestamp))
                set_members(db, identifier, body.get("members") or [])
                row = db.execute("SELECT * FROM scim_groups WHERE id=?", (identifier,)).fetchone(); result = _group(db, row)
        except KeyError: raise HTTPException(400, "displayName is required")
        except Exception as exc:
            if "UNIQUE constraint" in str(exc): raise HTTPException(409, "SCIM group already exists")
            raise
        return _response(result, 201, {"Location": f"/scim/v2/Groups/{identifier}", "ETag": 'W/"1"'})

    @router.get("/Groups/{identifier}")
    async def get_group(identifier: str) -> JSONResponse:
        with database.connect() as db:
            row = db.execute("SELECT * FROM scim_groups WHERE id=?", (identifier,)).fetchone()
            if not row: raise HTTPException(404, "SCIM group not found")
            result = _group(db, row)
        return _response(result, headers={"ETag": result["meta"]["version"]})

    async def update_group(identifier: str, request: Request, patch: bool) -> JSONResponse:
        body = await request.json()
        with database.transaction() as db:
            row = db.execute("SELECT * FROM scim_groups WHERE id=?", (identifier,)).fetchone()
            if not row: raise HTTPException(404, "SCIM group not found")
            display_name = row["display_name"]
            members = [{"value": value[0]} for value in db.execute("SELECT user_id FROM scim_group_members WHERE group_id=?", (identifier,))]
            operations = body.get("Operations", []) if patch else [{"op": "replace", "value": body}]
            for operation in operations:
                op = str(operation.get("op", "replace")).lower(); path = operation.get("path"); value = operation.get("value")
                filtered_member = re.fullmatch(r'members\[value eq "([^"\\]+)"\]', str(path or ""))
                if filtered_member and op == "remove":
                    members = [item for item in members if item["value"] != filtered_member.group(1)]
                    continue
                changes = value if path is None and isinstance(value, dict) else {path: value}
                if "displayName" in changes and op != "remove": display_name = str(changes["displayName"])
                if "members" in changes:
                    incoming = changes["members"] or []
                    if op == "add":
                        known = {item["value"] for item in members}; members.extend(item for item in incoming if item.get("value") not in known)
                    elif op == "remove":
                        remove = {item.get("value") for item in incoming}; members = [item for item in members if item["value"] not in remove]
                    else: members = incoming
            db.execute("UPDATE scim_groups SET display_name=?,raw_json=?,version=version+1,updated_at=? WHERE id=?",
                       (display_name, json.dumps(body), now_ms(), identifier))
            set_members(db, identifier, members)
            result = _group(db, db.execute("SELECT * FROM scim_groups WHERE id=?", (identifier,)).fetchone())
        return _response(result, headers={"ETag": result["meta"]["version"]})

    @router.put("/Groups/{identifier}")
    async def replace_group(identifier: str, request: Request) -> JSONResponse: return await update_group(identifier, request, False)

    @router.patch("/Groups/{identifier}")
    async def patch_group(identifier: str, request: Request) -> JSONResponse: return await update_group(identifier, request, True)

    @router.delete("/Groups/{identifier}", status_code=204)
    async def delete_group(identifier: str) -> Response:
        with database.transaction() as db: changed = db.execute("DELETE FROM scim_groups WHERE id=?", (identifier,)).rowcount
        if not changed: raise HTTPException(404, "SCIM group not found")
        return Response(status_code=204)

    return router
