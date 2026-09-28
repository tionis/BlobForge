import json
import sqlite3

import httpx
import pytest

from blobforge.enrichment.legacy import enrichment_recipe, enrichment_recipe_digest
from blobforge.enrichment_publish import publish_enrichments
from blobforge.mdaf import MdafSource, build_mdaf
from blobforge.mdaf.builder import activity, markdown_outline
from blobforge.server.app import create_app
from blobforge.server.config import ServerSettings

LEGACY_RECIPE = "blake3:8822289b4860301f73b64a2139a3559f2026793a48135fc13b83bc84a67b0c39"
ENRICHMENT_RECIPE = enrichment_recipe_digest(enrichment_recipe(validate_runtime=False))
SOURCE_KEY = "a" * 64
SOURCE_BLAKE3 = "1" * 64
ADMIN = {"Authorization": "Bearer client-secret"}


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _mdaf(path, *, recipe, derived_from=(), source_digest=f"blake3:{SOURCE_BLAKE3}",
          alternates=(f"sha256:{SOURCE_KEY}",)):
    text = "# Rulebook\n"
    return build_mdaf(
        path,
        text=text,
        title="Rulebook",
        sources=[MdafSource("document", "application/pdf", source_digest, tuple(alternates))],
        activities=[activity(
            activity_id="activity:enrich",
            kind="document-enrichment",
            tools=[{"name": "fixture", "version": "1.0.0"}],
            models=[],
            inputs=["source:document"],
            outputs=["text.md", "provenance.json", "outline.json"],
            parameters={"recipe_digest": recipe},
        )],
        producer={"name": "blobforge-test", "version": "1.0.0"},
        outline=markdown_outline(text),
        derived_from=derived_from,
    )


def _app_with_legacy_parent(tmp_path):
    """A finished legacy job whose sole artifact is a stored MDAF."""
    app = create_app(ServerSettings(data_dir=tmp_path / "data", client_token="client-secret", worker_tokens={}))
    database, storage = app.state.database, app.state.storage
    database.enqueue(SOURCE_KEY, {
        "media_type": "application/pdf", "priority": "3_normal", "original_name": "rulebook.pdf",
        "aliases": {"blake3": SOURCE_BLAKE3},
    })
    parent = _mdaf(tmp_path / "parent.mdaf", recipe=LEGACY_RECIPE)
    stored = storage.artifact_path(SOURCE_KEY, LEGACY_RECIPE, parent.identity)
    stored.parent.mkdir(parents=True)
    stored.write_bytes((tmp_path / "parent.mdaf").read_bytes())
    inspected = storage.inspect(stored)
    with database.transaction() as db:
        db.execute(
            """INSERT INTO artifacts(source_key,recipe_digest,identity,storage_path,media_type,
            artifact_type,size_bytes,sha256,blake3,provenance_json,created_at,legacy)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,1)""",
            (SOURCE_KEY, LEGACY_RECIPE, parent.identity, str(stored.relative_to(tmp_path / "data")),
             "application/zip", "mdaf/v1", inspected.size, inspected.sha256, inspected.blake3, "{}", 1),
        )
        db.execute("UPDATE jobs SET status='done',recipe_digest=? WHERE source_key=?", (LEGACY_RECIPE, SOURCE_KEY))
    return app, parent.identity


def _url(recipe=ENRICHMENT_RECIPE, select="true"):
    return f"/api/v1/admin/jobs/{SOURCE_KEY}/artifacts?recipe_digest={recipe}&select={select}"


@pytest.mark.anyio
async def test_import_publishes_derivative_and_selects_finished_legacy_job(tmp_path):
    app, parent_identity = _app_with_legacy_parent(tmp_path)
    child = _mdaf(tmp_path / "child.mdaf", recipe=ENRICHMENT_RECIPE, derived_from=[parent_identity])
    body = (tmp_path / "child.mdaf").read_bytes()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://testserver") as client:
        imported = await client.put(_url(), headers=ADMIN, content=body)
        assert imported.status_code == 200, imported.text
        assert imported.json() == {
            "action": "imported", "selected": True,
            "identity": child.identity, "parent_identity": parent_identity,
        }
        repeated = await client.put(_url(), headers=ADMIN, content=body)
        assert repeated.json()["action"] == "exists"
        assert repeated.json()["selected"] is False

        artifacts = (await client.get(f"/api/v1/jobs/{SOURCE_KEY}/artifacts", headers=ADMIN)).json()["artifacts"]
    job = app.state.database.get_job(SOURCE_KEY)
    assert job["status"] == "done"
    assert job["recipe_digest"] == ENRICHMENT_RECIPE
    derived = next(item for item in artifacts if item["recipe_digest"] == ENRICHMENT_RECIPE)
    assert derived["identity"] == child.identity
    assert derived["legacy"] is False
    assert derived["provenance"]["execution_mode"] == "import"
    assert derived["provenance"]["parent_artifact_identity"] == parent_identity
    assert (tmp_path / "data" / derived["storage_path"]).read_bytes() == body


@pytest.mark.anyio
async def test_import_does_not_override_a_newer_current_recipe(tmp_path):
    app, parent_identity = _app_with_legacy_parent(tmp_path)
    with app.state.database.transaction() as db:
        db.execute("UPDATE jobs SET recipe_digest='blake3:' || ? WHERE source_key=?", ("f" * 64, SOURCE_KEY))
    _mdaf(tmp_path / "child.mdaf", recipe=ENRICHMENT_RECIPE, derived_from=[parent_identity])
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://testserver") as client:
        imported = await client.put(_url(), headers=ADMIN, content=(tmp_path / "child.mdaf").read_bytes())
    assert imported.json()["action"] == "imported"
    assert imported.json()["selected"] is False
    assert app.state.database.get_job(SOURCE_KEY)["recipe_digest"] == "blake3:" + "f" * 64


@pytest.mark.anyio
async def test_import_rejects_unsafe_derivatives_without_storing_them(tmp_path):
    app, parent_identity = _app_with_legacy_parent(tmp_path)
    orphan = tmp_path / "orphan.mdaf"
    _mdaf(orphan, recipe=ENRICHMENT_RECIPE, derived_from=["blake3:" + "2" * 64])
    other_source = tmp_path / "other-source.mdaf"
    _mdaf(other_source, recipe=ENRICHMENT_RECIPE, derived_from=[parent_identity],
          source_digest="blake3:" + "3" * 64, alternates=())
    wrong_recipe = tmp_path / "wrong-recipe.mdaf"
    _mdaf(wrong_recipe, recipe=LEGACY_RECIPE, derived_from=[parent_identity])
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://testserver") as client:
        for path in (orphan, other_source, wrong_recipe):
            response = await client.put(_url(), headers=ADMIN, content=path.read_bytes())
            assert response.status_code == 422, response.text
        not_importable = await client.put(_url(recipe=LEGACY_RECIPE), headers=ADMIN, content=orphan.read_bytes())
        assert not_importable.status_code == 409
        anonymous = await client.put(_url(), content=orphan.read_bytes())
        assert anonymous.status_code == 401
    pending = tmp_path / "data" / "pending"
    assert not [path for path in pending.rglob("*") if path.is_file()]
    assert len(app.state.database.artifacts(SOURCE_KEY)) == 1


@pytest.mark.anyio
async def test_import_only_recipe_cannot_queue_source_conversions(tmp_path):
    app, _ = _app_with_legacy_parent(tmp_path)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://testserver") as client:
        recipes = (await client.get("/api/v1/recipes", headers=ADMIN)).json()["recipes"]
        installed = next(item for item in recipes if item["recipe_digest"] == ENRICHMENT_RECIPE)
        assert installed["input_kinds"] == ["artifact"]
        converted = await client.post(
            f"/api/v1/jobs/{SOURCE_KEY}/convert", headers=ADMIN,
            json={"recipe_digest": ENRICHMENT_RECIPE},
        )
        assert converted.status_code == 409
        uploaded = await client.post(
            f"/api/v1/admin/uploads?filename=new.pdf&media_type=application/pdf&recipe_digest={ENRICHMENT_RECIPE}",
            headers=ADMIN, content=b"%PDF-1.7\n",
        )
        assert uploaded.status_code == 409
    assert app.state.database.get_job(SOURCE_KEY)["recipe_digest"] == LEGACY_RECIPE


class _FakeCoordinator:
    def __init__(self, parent_identity, job_recipe=LEGACY_RECIPE):
        self.artifacts = [{"identity": parent_identity, "recipe_digest": LEGACY_RECIPE}]
        self.job = {"status": "done", "recipe_digest": job_recipe}
        self.imports = []

    def list_artifacts(self, key):
        return list(self.artifacts)

    def get_job(self, key):
        return dict(self.job)

    def import_artifact(self, key, recipe, path, *, select):
        self.imports.append((key, recipe, path, select))
        from blobforge.mdaf import validate_mdaf
        identity = validate_mdaf(path).identity
        self.artifacts.append({"identity": identity, "recipe_digest": recipe})
        if select:
            self.job["recipe_digest"] = recipe
        return {"action": "imported", "selected": select, "identity": identity}


def _workspace(tmp_path, *, derived_from=None):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    parent = _mdaf(tmp_path / "parent.mdaf", recipe=LEGACY_RECIPE)
    output = workspace / "child.mdaf"
    child = _mdaf(output, recipe=ENRICHMENT_RECIPE, derived_from=derived_from or [parent.identity])
    connection = sqlite3.connect(workspace / "catalog.sqlite3")
    connection.execute("""CREATE TABLE legacy_enrichments(legacy_sha256 TEXT, recipe_digest TEXT,
        base_mdaf_identity TEXT, status TEXT, output_path TEXT, mdaf_identity TEXT)""")
    connection.execute("INSERT INTO legacy_enrichments VALUES(?,?,?,?,?,?)",
                       (SOURCE_KEY, ENRICHMENT_RECIPE, parent.identity, "converted", str(output), child.identity))
    connection.commit()
    connection.close()
    return workspace, parent.identity


def test_publisher_dry_run_sends_nothing_and_resumed_execute_is_idempotent(tmp_path):
    workspace, parent_identity = _workspace(tmp_path)
    coordinator = _FakeCoordinator(parent_identity)

    planned = publish_enrichments(workspace, coordinator)
    assert planned.counts == {"import": 1}
    assert coordinator.imports == []

    executed = publish_enrichments(workspace, coordinator, execute=True)
    assert executed.counts == {"imported": 1, "selected": 1}
    assert coordinator.job["recipe_digest"] == ENRICHMENT_RECIPE

    resumed = publish_enrichments(workspace, coordinator, execute=True)
    assert resumed.counts == {"present": 1}
    assert len(coordinator.imports) == 1
    assert json.loads(json.dumps(resumed.as_dict()))["errors"] == []


def test_publisher_refuses_items_whose_base_is_not_in_production(tmp_path):
    workspace, _ = _workspace(tmp_path)
    coordinator = _FakeCoordinator("blake3:" + "9" * 64)
    summary = publish_enrichments(workspace, coordinator, execute=True)
    assert summary.counts == {"error": 1}
    assert "base artifact" in summary.errors[0]["error"]
    assert coordinator.imports == []
