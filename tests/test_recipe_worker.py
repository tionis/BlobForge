import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from blobforge.converters import ConverterRunResult, ProviderProbe
from blobforge.recipe_runtime import (
    AdapterRecipe,
    datalab_wiki_v1_recipe,
    mistral_wiki_v3_recipe,
)
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
        self.quota_authorization = None
        self.quota_settled = []

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

    def deregister_worker(self, *args, **kwargs):
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

    def reserve_quota(self, key, **kwargs):
        if self.quota_authorization is not None:
            return self.quota_authorization
        return {"authorized": True, "reservation": {"id": f"qres-{key}"}}

    def settle_quota(self, identifier, report):
        self.quota_settled.append((identifier, report))
        return {"id": identifier, "state": report["state"]}


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


def test_worker_reprocesses_artifact_input_without_running_converter():
    base = _recipe("target-v1", "application/pdf")
    recipe = AdapterRecipe(
        **{
            **base.__dict__,
            "input_kinds": ("source", "artifact"),
        }
    )
    job = _job("a", recipe, "application/pdf")
    job.update(
        {
            "input_kind": "artifact",
            "parent_recipe_digest": "parent-v1",
            "source": b"parent-mdaf",
            "input": {"kind": "artifact"},
        }
    )
    coordinator = FakeCoordinator([job])
    calls = []

    def reprocess(parent, target, output):
        calls.append((Path(parent).suffix, Path(parent).read_bytes(), target))
        Path(output).write_bytes(b"mdaf")
        return SimpleNamespace(identity="derived-identity")

    worker = RecipeWorker(
        coordinator,
        [recipe],
        converter=lambda *args, **kwargs: pytest.fail("converter must not run"),
        reprocessor=reprocess,
        heartbeat_interval=3600,
    )
    worker.register()
    outcome = worker.process_once()
    assert outcome.success
    assert calls == [(".mdaf", b"parent-mdaf", recipe.recipe)]
    completed = coordinator.completed[0][1]["result"]
    assert completed["execution_mode"] == "artifact"
    assert completed["logical_identity"] == "derived-identity"


def test_mistral_canary_runtime_is_exact_and_secret_free(tmp_path):
    recipe = mistral_wiki_v3_recipe(
        max_pages=250,
        max_cost_usd=1.0,
        response_cache=tmp_path / "cache",
        api_rights_confirmed=True,
    )
    assert recipe.recipe_digest == (
        "blake3:3f504116b8747b311f07310ea48b53eddaf4a37330ffe6c29e015f06d4185139"
    )
    assert recipe.artifact_type == "mdaf/v1"
    assert "MISTRAL_API_KEY" not in recipe.environment
    assert recipe.deployment_status == "canary"
    assert recipe.input_kinds == ("source", "artifact")

    cached = mistral_wiki_v3_recipe(
        max_pages=250,
        max_cost_usd=1.0,
        response_cache=tmp_path / "cache",
        api_rights_confirmed=True,
        cache_only=True,
    )
    assert cached.environment["MISTRAL_API_KEY"] == ""

    datalab = datalab_wiki_v1_recipe(
        max_pages=250,
        max_cost_usd=1.0,
        response_cache=tmp_path / "datalab-cache",
        api_rights_confirmed=True,
    )
    assert datalab.recipe_digest == (
        "blake3:fcc851f8e84d0c22e44200208ccd50d76319c5aec6d3bc1de6bc9b026d3ac502"
    )
    assert datalab.provider == "datalab"
    assert datalab.provider_account == "datalab:primary"
    assert datalab.input_kinds == ("source",)


def test_hosted_worker_reserves_and_settles_before_completion(tmp_path):
    base = _recipe("hosted-v1", "application/pdf")
    recipe = AdapterRecipe(
        **{
            **base.__dict__,
            "provider": "test-provider",
            "provider_account": "test:primary",
        }
    )
    coordinator = FakeCoordinator([_job("paid", recipe, "application/pdf")])
    probe = ProviderProbe(
        provider="test-provider",
        account_key="test:primary",
        checkpoint_key="checkpoint:paid",
        cache_hit=False,
        requests=1,
        pages=8,
        estimated_micro_usd=32_000,
        raw={
            "contract": "dev.tionis.blobforge.provider-probe/v1",
            "provider": "test-provider",
            "account_key": "test:primary",
            "checkpoint_key": "checkpoint:paid",
            "cache_hit": False,
            "requests": 1,
            "pages": 8,
            "estimated_micro_usd": 32_000,
        },
    )

    def convert(_command, _source, output, **kwargs):
        report = {
            "contract": "dev.tionis.blobforge.provider-attempt/v1",
            "reservation_id": kwargs["reservation_id"],
            "provider": "test-provider",
            "account_key": "test:primary",
            "checkpoint_key": "checkpoint:paid",
            "state": "committed",
            "requests": 1,
            "pages": 8,
            "list_micro_usd": 32_000,
            "billed_micro_usd": 0,
            "credits_micro_usd": 32_000,
        }
        Path(kwargs["attempt_report_path"]).write_text(json.dumps(report))
        Path(output).write_bytes(b"mdaf")
        return ConverterRunResult(Path(output), "paid-identity", 1.0, (), report)

    worker = RecipeWorker(
        coordinator,
        [recipe],
        converter=convert,
        prober=lambda *_args, **_kwargs: probe,
        heartbeat_interval=3600,
    )
    worker.register()
    outcome = worker.process_once()
    assert outcome.success
    assert coordinator.quota_settled[0][0] == "qres-paid"
    assert coordinator.quota_settled[0][1]["state"] == "committed"
    assert coordinator.completed[0][0] == "paid"


def test_hosted_worker_returns_deferred_outcome_without_failure():
    base = _recipe("hosted-v1", "application/pdf")
    recipe = AdapterRecipe(
        **{
            **base.__dict__,
            "provider": "test-provider",
            "provider_account": "test:primary",
        }
    )
    coordinator = FakeCoordinator([_job("deferred", recipe, "application/pdf")])
    coordinator.quota_authorization = {
        "authorized": False,
        "reason": "quota exhausted",
        "not_before": 123,
    }
    probe = ProviderProbe(
        "test-provider",
        "test:primary",
        "checkpoint:deferred",
        False,
        1,
        8,
        32_000,
        {
            "contract": "dev.tionis.blobforge.provider-probe/v1",
            "provider": "test-provider",
            "account_key": "test:primary",
            "checkpoint_key": "checkpoint:deferred",
            "cache_hit": False,
            "requests": 1,
            "pages": 8,
            "estimated_micro_usd": 32_000,
        },
    )
    worker = RecipeWorker(
        coordinator,
        [recipe],
        converter=lambda *_args, **_kwargs: pytest.fail("deferred work must not convert"),
        prober=lambda *_args, **_kwargs: probe,
    )
    assert worker.run(run_once=True) == 0
    assert coordinator.failed == []
