"""Shared pytest fixtures."""

import pytest

from analysis_pipeline_utils.settings import get_settings


@pytest.fixture(autouse=True)
def _default_codeocean_api_token(monkeypatch):
    """
    `codeocean_api_token` is a required field on `PipelineEnvSettings`, so
    provide a default value for tests that don't care about credentials.
    Tests that specifically exercise missing/invalid credentials can still
    override or clear it (e.g. via `patch.dict(os.environ, ..., clear=True)`).
    """
    monkeypatch.setenv("CODEOCEAN_API_TOKEN", "test-token")


@pytest.fixture(autouse=True)
def _clear_settings_cache():
    """
    `get_settings` caches settings instances per-class via `lru_cache`, so
    without clearing it between tests, one test's environment/settings would
    leak into another via the cached singleton.
    """
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()
