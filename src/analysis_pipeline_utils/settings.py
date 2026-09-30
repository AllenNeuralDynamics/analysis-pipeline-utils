"""
Pydantic settings model for the standard environment variables used to
configure an analysis pipeline (see `nextflow.config` in
aind-analysis-pipeline-template).
"""

import logging
from functools import lru_cache
from typing import List, Optional, Type, TypeVar

from codeocean import CodeOcean
from pydantic import Field, SecretStr, ValidationError, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseSettings)


class PipelineEnvSettings(BaseSettings):
    """
    Loads the environment variables set in an analysis pipeline's
    `nextflow.config` (or a wrapper capsule's `settings.env`).

    Field names are matched case-insensitively to environment variables,
    e.g. `docdb_host` reads from the `DOCDB_HOST` environment variable.
    """

    # Values are loaded with the following priority (highest to lowest):
    # init kwargs > pipeline env (nextflow.config) > environment variables > `settings.env` file > defaults
    model_config = SettingsConfigDict(
        env_file="settings.env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Standard across all analysis pipelines
    docdb_database: str = Field(
        default="analysis",
        description="DocDB database name",
    )
    docdb_host: str = Field(
        default="api.allenneuraldynamics.org",
        description="DocDB host",
    )
    codeocean_domain: str = Field(
        # in pipeline context can be set from GIT_HOST
        # alias="git_host",
        default="codeocean.allenneuraldynamics.org",
        description="Code Ocean domain",
    )

    # Pipeline-specific, typically set per pipeline
    docdb_collection: Optional[str] = Field(
        default=None,
        description="DocDB collection name for this pipeline",
    )
    analysis_bucket: Optional[str] = Field(
        default=None,
        description="S3 bucket to copy analysis results to",
    )
    codeocean_api_token: SecretStr = Field(
        # note: in pipeline could be extracted from GIT_ACCESS_TOKEN
        description="API token used to authenticate with Code Ocean",
    )
    codeocean_email: Optional[str] = Field(
        default=None,
        description="Email address associated with the pipeline run",
    )
    co_capsule_branch: Optional[str] = Field(
        default=None,
        description="Branch of the wrapper capsule to run, if not main",
    )
    patch_commits: Optional[List[str]] = Field(
        default=None,
        description="Commit hashes of patch commits that should not "
        "trigger reprocessing of existing results",
    )

    # Set by Code Ocean at runtime
    co_computation_id: Optional[str] = Field(
        default=None,
        description="Code Ocean computation ID for the current run",
    )
    co_pipeline_id: Optional[str] = Field(
        default=None,
        description="Code Ocean capsule ID of the pipeline, if running as one",
    )
    co_capsule_id: Optional[str] = Field(
        default=None,
        description="Code Ocean capsule ID of the current capsule run",
    )

    @field_validator("patch_commits", mode="before")
    @classmethod
    def _split_patch_commits(cls, value):
        """Allow PATCH_COMMITS to be provided as a comma-separated string"""
        if isinstance(value, str):
            return [commit.strip() for commit in value.split(",") if commit.strip()]
        return value

    @model_validator(mode="after")
    def _fill_codeocean_email(self) -> "PipelineEnvSettings":
        """
        If codeocean_email is not set, fill it in by querying the Code Ocean
        API for the current computation's owner_email.
        """
        if (
            self.codeocean_email is None
            and self.co_computation_id
            and self.codeocean_api_token
        ):
            try:
                client = CodeOcean(
                    domain=f"https://{self.codeocean_domain}",
                    token=self.codeocean_api_token.get_secret_value(),
                )
                computation = client.computations.get_computation(
                    self.co_computation_id
                )
                self.codeocean_email = computation.owner_email
            except Exception:
                logger.warning(
                    "Failed to fetch owner_email from Code Ocean API for "
                    f"computation {self.co_computation_id}",
                    exc_info=True,
                )
        return self

@lru_cache(maxsize=None)
def get_settings(cls: Type[T]) -> T:
    """
    Get a cached, lazily-constructed singleton instance of a `BaseSettings`
    subclass, keyed by the class itself.

    This allows different entry points to define their own settings model
    (e.g. a subclass of `PipelineEnvSettings` that adds CLI-specific fields
    for a particular script) while still sharing a single cached instance
    per class within a process.

    Parameters
    ----------
    cls : Type[T]
        A `BaseSettings` subclass (e.g. `PipelineEnvSettings`) to construct.

    Returns
    -------
    T
        The cached instance of `cls`.

    Raises
    ------
    ValueError
        If required environment variables are missing, with a message
        naming the missing environment variable(s) rather than pydantic's
        default per-field "Field required" errors.
    """
    try:
        return cls()
    except ValidationError as e:
        missing = [
            str(err["loc"][0]).upper() for err in e.errors() if err["type"] == "missing"
        ]
        if missing:
            raise ValueError(
                "Missing required environment variable(s): " + ", ".join(missing)
            ) from e
        raise

    return cls()
