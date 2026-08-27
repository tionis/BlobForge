import hashlib
import io
import zipfile

import httpx
import pytest
from authlib.integrations.base_client.errors import OAuthError

import blobforge.server.app as server_app
from blobforge.server.app import create_app
from blobforge.server.config import ServerSettings


def _zip_bytes() -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("content.md", "# Local backend\n")
    return output.getvalue()


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_local_backend_ingest_claim_complete_download(tmp_path):
    app = create_app(ServerSettings(
        data_dir=tmp_path,
        client_token="client-secret",
        worker_tokens={"pdf-worker": "worker-secret"},
        lease_seconds=60,
    ))
    client_headers = {"Authorization": "Bearer client-secret"}
    worker_headers = {"Authorization": "Bearer worker-secret"}
    source = b"%PDF-1.7\nlocal fixture\n"
    source_hash = hashlib.sha256(source).hexdigest()

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        assert (await client.get("/api/v1/health")).json()["backend"] == "sqlite-filesystem"
        landing = await client.get("/", headers=client_headers)
        assert landing.status_code == 200
        assert "BlobForge" in landing.text
        assert "API documentation" in landing.text
        assert landing.headers["cache-control"] == "private, no-store"
        transfer = (await client.post(
            f"/api/v1/jobs/{source_hash}/raw-upload-url",
            headers=client_headers,
            json={"digest_algorithm": "sha256", "media_type": "application/pdf"},
        )).json()
        uploaded = await client.put(transfer["url"], content=source)
        assert uploaded.status_code == 200

        queued = await client.put(
            f"/api/v1/jobs/{source_hash}",
            headers=client_headers,
            json={
                "priority": "1_urgent",
                "original_name": "fixture.pdf",
                "size_bytes": len(source),
                "paths": ["fixture.pdf"],
                "tags": ["test"],
                "digest_algorithm": "sha256",
                "digest": source_hash,
                "media_type": "application/pdf",
            },
        )
        assert queued.status_code == 200

        registration = await client.post(
            "/api/v1/workers/register",
            headers=worker_headers,
            json={"worker_id": "pdf-worker", "hostname": "test"},
        )
        assert registration.status_code == 200
        claim = (await client.post(
            "/api/v1/jobs/claim",
            headers=worker_headers,
            json={
                "worker_id": "pdf-worker",
                "priorities": ["1_urgent"],
                "recipe_digest": "recipe-v1",
                "recipe": {"engine": "fixture"},
                "accepted_media_types": ["application/pdf"],
            },
        )).json()["job"]
        assert claim["hash"] == source_hash
        assert (await client.get(claim["input"]["url"])).content == source

        artifact = _zip_bytes()
        output_transfer = (await client.post(
            f"/api/v1/jobs/{source_hash}/upload-url",
            headers=worker_headers,
            json={"worker_id": "pdf-worker", "lease_token": claim["lease_token"]},
        )).json()
        assert (await client.put(output_transfer["url"], content=artifact)).status_code == 200
        completed = await client.post(
            f"/api/v1/jobs/{source_hash}/complete",
            headers=worker_headers,
            json={
                "worker_id": "pdf-worker",
                "lease_token": claim["lease_token"],
                "result": {"recipe_digest": "recipe-v1", "media_type": "application/zip"},
            },
        )
        assert completed.status_code == 200
        status = (await client.post(
            "/api/v1/jobs/status", headers=client_headers, json={"hashes": [source_hash]}
        )).json()["results"][source_hash]
        assert status["done"] is True
        download = (await client.post(
            f"/api/v1/jobs/{source_hash}/download-url",
            headers=client_headers,
            json={"recipe_digest": "recipe-v1"},
        )).json()["url"]
        assert (await client.get(download)).content == artifact
        snapshot = (await client.get("/api/v1/snapshot", headers=client_headers)).json()
        assert snapshot["counts"]["done"] == 1
        assert snapshot["backend"] == "sqlite-filesystem"

        requeue = await client.post(
            f"/api/v1/jobs/{source_hash}/convert",
            headers=client_headers,
            json={"recipe_digest": "recipe-v2"},
        )
        assert requeue.json()["action"] == "queued"
        assert (await client.get(
            f"/api/v1/jobs/{source_hash}", headers=client_headers
        )).json()["status"] == "todo"


@pytest.mark.anyio
async def test_root_redirects_unauthenticated_browser_to_oidc(tmp_path):
    app = create_app(ServerSettings(
        data_dir=tmp_path,
        client_token="client-secret",
        worker_tokens={},
        public_url="https://blobforge.example",
        oidc_issuer="https://auth.example/application/o/blobforge/",
        oidc_client_id="blobforge",
        oidc_client_secret="oidc-secret",
        session_secret="session-secret",
    ))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="https://blobforge.example",
        follow_redirects=False,
    ) as client:
        response = await client.get("/")
    assert response.status_code == 307
    assert response.headers["location"] == "/auth/login"


@pytest.mark.anyio
async def test_browser_oidc_denial_is_friendly_while_api_errors_remain_json(
    tmp_path, monkeypatch
):
    class FakeOIDC:
        async def authorize_access_token(self, _request):
            return {"userinfo": {"sub": "not-provisioned"}}

    class FakeOAuth:
        def __init__(self):
            self.oidc = FakeOIDC()

        def register(self, **_kwargs):
            return None

    monkeypatch.setattr(server_app, "OAuth", FakeOAuth)
    app = create_app(ServerSettings(
        data_dir=tmp_path,
        client_token="client-secret",
        worker_tokens={},
        public_url="https://blobforge.example",
        oidc_issuer="https://auth.example/application/o/blobforge/",
        oidc_client_id="blobforge",
        oidc_client_secret="oidc-secret",
        session_secret="session-secret",
    ))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="https://blobforge.example"
    ) as client:
        browser = await client.get("/auth/callback", headers={"Accept": "text/html"})
        api = await client.get("/api/v1/me", headers={"Accept": "application/json"})

    assert browser.status_code == 403
    assert browser.headers["content-type"].startswith("text/html")
    assert browser.headers["cache-control"] == "private, no-store"
    assert "Access is not provisioned" in browser.text
    assert "Start a new sign-in" in browser.text
    assert "OIDC identity is not" not in browser.text
    assert api.status_code == 401
    assert api.json() == {"detail": "valid client token or provisioned OIDC session required"}


@pytest.mark.anyio
async def test_reused_oidc_callback_renders_recovery_page(tmp_path, monkeypatch):
    class FakeOIDC:
        async def authorize_access_token(self, _request):
            raise OAuthError("invalid_grant", "authorization code was already used")

    class FakeOAuth:
        def __init__(self):
            self.oidc = FakeOIDC()

        def register(self, **_kwargs):
            return None

    monkeypatch.setattr(server_app, "OAuth", FakeOAuth)
    app = create_app(ServerSettings(
        data_dir=tmp_path,
        client_token="client-secret",
        worker_tokens={},
        public_url="https://blobforge.example",
        oidc_issuer="https://auth.example/application/o/blobforge/",
        oidc_client_id="blobforge",
        oidc_client_secret="oidc-secret",
        session_secret="session-secret",
    ))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="https://blobforge.example"
    ) as client:
        response = await client.get("/auth/callback", headers={"Accept": "text/html"})

    assert response.status_code == 400
    assert "Sign-in could not be completed" in response.text
    assert "expired or was already used" in response.text
    assert "authorization code was already used" not in response.text


@pytest.mark.anyio
async def test_pdf_worker_does_not_claim_other_media(tmp_path):
    app = create_app(ServerSettings(
        data_dir=tmp_path,
        client_token="client-secret",
        worker_tokens={"pdf-worker": "worker-secret"},
    ))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        source = b"fLaC synthetic"
        source_hash = hashlib.sha256(source).hexdigest()
        transfer = (await client.post(
            f"/api/v1/jobs/{source_hash}/raw-upload-url",
            headers={"Authorization": "Bearer client-secret"},
            json={"media_type": "audio/flac"},
        )).json()
        assert (await client.put(transfer["url"], content=source)).status_code == 200
        await client.put(
            f"/api/v1/jobs/{source_hash}",
            headers={"Authorization": "Bearer client-secret"},
            json={"media_type": "audio/flac", "priority": "3_normal"},
        )
        response = await client.post(
            "/api/v1/jobs/claim",
            headers={"Authorization": "Bearer worker-secret"},
            json={
                "worker_id": "pdf-worker",
                "priorities": ["3_normal"],
                "accepted_media_types": ["application/pdf"],
            },
        )
        assert response.status_code == 200
        assert response.json()["job"] is None


@pytest.mark.anyio
async def test_multipurpose_worker_claims_capability_and_backend_can_be_selected(tmp_path):
    app = create_app(ServerSettings(data_dir=tmp_path, client_token="client-secret",
                                    worker_tokens={"mixed": "worker-secret"}))
    client_headers = {"Authorization": "Bearer client-secret"}
    worker_headers = {"Authorization": "Bearer worker-secret"}
    capabilities = [
        {"backend": "marker", "recipe_digest": "marker-v1", "recipe": {"engine": "marker"},
         "media_types": ["application/pdf"], "artifact_type": "mdaf/v1"},
        {"backend": "whisper", "recipe_digest": "whisper-v1", "recipe": {"engine": "whisper"},
         "media_types": ["audio/flac"], "artifact_type": "mdaf/v1"},
    ]
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://testserver") as client:
        registered = await client.post("/api/v1/workers/register", headers=worker_headers,
                                       json={"worker_id": "mixed", "capabilities": capabilities})
        assert registered.status_code == 200
        for content, media_type, priority in ((b"%PDF-x", "application/pdf", "3_normal"),
                                               (b"fLaC-x", "audio/flac", "1_urgent")):
            digest = hashlib.sha256(content).hexdigest()
            transfer = (await client.post(f"/api/v1/jobs/{digest}/raw-upload-url", headers=client_headers,
                                          json={"media_type": media_type})).json()
            await client.put(transfer["url"], content=content)
            await client.put(f"/api/v1/jobs/{digest}", headers=client_headers,
                             json={"media_type": media_type, "priority": priority})
        claim = (await client.post("/api/v1/jobs/claim", headers=worker_headers,
                                   json={"worker_id": "mixed", "priorities": ["1_urgent", "3_normal"],
                                         "capabilities": capabilities})).json()["job"]
        assert claim["media_type"] == "audio/flac"
        assert claim["capability"]["backend"] == "whisper"
        recipes = (await client.get("/api/v1/recipes", headers=client_headers,
                                    params={"media_type": "application/pdf"})).json()["recipes"]
        assert [item["backend"] for item in recipes] == ["marker"]


@pytest.mark.anyio
async def test_scim_provisioning_drives_oidc_principal_roles(tmp_path):
    app = create_app(ServerSettings(data_dir=tmp_path, client_token="client-secret", worker_tokens={},
                                    scim_token="scim-secret"))
    headers = {"Authorization": "Bearer scim-secret"}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://testserver") as client:
        config = await client.get("/scim/v2/ServiceProviderConfig", headers=headers)
        assert config.status_code == 200
        created_user = (await client.post("/scim/v2/Users", headers=headers, json={
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
            "externalId": "oidc-subject", "userName": "eric", "active": True,
        })).json()
        created_group = (await client.post("/scim/v2/Groups", headers=headers, json={
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
            "displayName": "blobforge-admin", "members": [{"value": created_user["id"]}],
        })).json()
        principal = app.state.database.oidc_principal("oidc-subject", app.state.settings.role_groups)
        assert principal and principal["roles"] == ["admin"]
        disabled = await client.patch(f"/scim/v2/Users/{created_user['id']}", headers=headers, json={
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
            "Operations": [{"op": "replace", "path": "active", "value": False}],
        })
        assert disabled.status_code == 200
        assert app.state.database.oidc_principal("oidc-subject", app.state.settings.role_groups) is None
        assert created_group["displayName"] == "blobforge-admin"


def test_worker_environment_mapping_is_authoritative(tmp_path):
    create_app(ServerSettings(
        data_dir=tmp_path,
        client_token="client-secret",
        worker_tokens={"retired": "old-token"},
    ))
    replacement = create_app(ServerSettings(
        data_dir=tmp_path,
        client_token="client-secret",
        worker_tokens={"active": "new-token"},
    ))
    database = replacement.state.database
    assert database.worker_for_token("old-token") is None
    assert database.worker_for_token("new-token") == "active"
