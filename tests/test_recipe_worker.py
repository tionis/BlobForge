from pathlib import Path

from blobforge.converters import ConverterRunResult
from blobforge.recipe_runtime import AdapterRecipe, mistral_wiki_v2_recipe
from blobforge.recipe_worker import RecipeWorker


def _recipe(digest, media_type):
    return AdapterRecipe(
        key=digest,
        backend=f"backend-{digest}",
        recipe={"engine": f"backend-{digest}"},
        recipe_digest=digest,
        media_types=(media_type,),
        artifact_type="mdaf/v1",
        input_suffix=".pdf" if media_type == "application/pdf" else ".flac",
        command=("adapter", digest),
        parameters={"recipe_digest": digest},
        environment={},
        deployment_status="test",
    )


class FakeCoordinator:
    def __init__(self, jobs):
        self.jobs = list(jobs)
        self.registered = None
        self.completed = []
        self.failed = []
        self.released = []

    def worker_identity(self):
        return "mixed-worker"

    def register_worker(self, worker_id, metadata):
        self.registered = (worker_id, metadata)
        return {}

    def claim_job(self, worker_id, priorities, **kwargs):
        self.claim_capabilities = kwargs["capabilities"]
        return self.jobs.pop(0) if self.jobs else None

    def heartbeat(self, *args, **kwargs):
        pass

    def worker_heartbeat(self, *args, **kwargs):
        pass

    def download_job_input(self, job, path):
        Path(path).write_bytes(job.get("source", b"source"))

    def upload_job_output(self, key, path, **kwargs):
        assert Path(path).read_bytes() == b"mdaf"

    def complete(self, key, **kwargs):
        self.completed.append((key, kwargs))

    def fail(self, key, **kwargs):
        self.failed.append((key, kwargs))

    def release(self, key, **kwargs):
        self.released.append((key, kwargs))


def _job(key, recipe, media_type):
    return {
        "hash": key,
        "lease_token": f"lease-{key}",
        "media_type": media_type,
        "capability": recipe.capability(),
    }


def test_worker_advertises_and_dispatches_alternating_media_recipes(tmp_path):
    pdf = _recipe("pdf-v1", "application/pdf")
    audio = _recipe("audio-v1", "audio/flac")
    coordinator = FakeCoordinator([_job("a", audio, "audio/flac"), _job("b", pdf, "application/pdf")])
    calls = []

    def convert(command, source, output, **kwargs):
        calls.append(
            (command[-1], Path(source).suffix, Path(source).read_bytes(), kwargs["parameters"])
        )
        Path(output).write_bytes(b"mdaf")
        return ConverterRunResult(Path(output), f"identity-{command[-1]}", 1.5, ())

    worker = RecipeWorker(coordinator, [pdf, audio], converter=convert, heartbeat_interval=3600)
    assert worker.register() == "mixed-worker"
    assert len(coordinator.registered[1]["capabilities"]) == 2
    assert worker.process_once().success
    assert worker.process_once().success
    assert [call[0] for call in calls] == ["audio-v1", "pdf-v1"]
    assert [call[1] for call in calls] == [".flac", ".pdf"]
    assert [value[0] for value in coordinator.completed] == ["a", "b"]
    first_result = coordinator.completed[0][1]["result"]
    assert first_result["logical_identity"] == "identity-audio-v1"
    assert first_result["converter_backend"] == "backend-audio-v1"
    assert first_result["artifact_type"] == "mdaf/v1"


def test_worker_releases_unknown_claim_without_executing():
    known = _recipe("known", "application/pdf")
    unknown = _recipe("unknown", "application/pdf")
    coordinator = FakeCoordinator([_job("a", unknown, "application/pdf")])
    worker = RecipeWorker(coordinator, [known], converter=lambda *a, **k: None)
    worker.register()
    outcome = worker.process_once()
    assert outcome.success is False
    assert coordinator.released[0][0] == "a"
    assert coordinator.completed == []


def test_worker_reports_adapter_failure_and_retains_recipe_context():
    recipe = _recipe("pdf-v1", "application/pdf")
    coordinator = FakeCoordinator([_job("a", recipe, "application/pdf")])

    def fail(*args, **kwargs):
        raise RuntimeError("broken adapter")

    worker = RecipeWorker(coordinator, [recipe], converter=fail)
    worker.register()
    outcome = worker.process_once()
    assert not outcome.success
    assert coordinator.failed[0][1]["context"]["recipe_digest"] == "pdf-v1"


def test_mistral_canary_runtime_is_exact_and_secret_free(tmp_path):
    recipe = mistral_wiki_v2_recipe(
        max_pages=250,
        max_cost_usd=1.0,
        response_cache=tmp_path / "cache",
        api_rights_confirmed=True,
    )
    assert recipe.recipe_digest == (
        "blake3:bdd3e060e88f64277834245a42528a54b6b077774123c3806bdd827cf8ea3026"
    )
    assert recipe.artifact_type == "mdaf/v1"
    assert "MISTRAL_API_KEY" not in recipe.environment
    assert recipe.deployment_status == "canary"

    cached = mistral_wiki_v2_recipe(
        max_pages=250,
        max_cost_usd=1.0,
        response_cache=tmp_path / "cache",
        api_rights_confirmed=True,
        cache_only=True,
    )
    assert cached.environment["MISTRAL_API_KEY"] == ""
