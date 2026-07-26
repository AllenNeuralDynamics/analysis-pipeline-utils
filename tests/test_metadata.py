"""Tests functions for processing metadata."""

import os
from datetime import datetime
from types import SimpleNamespace as MockModel
from unittest.mock import Mock, patch

import pytest
from codeocean.computation import Computation, ComputationState, Param

from analysis_pipeline_utils.analysis_dispatch_model import (
    AnalysisDispatchModel,
)
from analysis_pipeline_utils.metadata import (
    _initialize_codeocean_client,
    _run_git_command,
    update_analysis_process,
    docdb_record_exists,
    extract_parameters,
    get_capsule_version_ignoring_patches,
    get_commits_since_release,
    get_release_version_for_major,
    get_data_asset_url,
    get_docdb_records,
    get_metadata_for_records,
)


# Fixtures for common test data
@pytest.fixture
def mock_computation():
    """
    Mocks the computation record
    """
    computation = Computation(
        id="test_computation_id",
        parameters=[
            Param(name="param1", value="value1"),
            Param(name="param2", value="value2"),
        ],
        created=1622764800,
        run_time=3600,
        name="test_capsule",
        state=ComputationState.Running,
    )
    return computation


@pytest.fixture
def mock_code_ocean_client():
    """
    Mocks the code ocean client
    """
    client = Mock()
    client.data_assets.get_data_asset.return_value = Mock(
        source_bucket=Mock(origin="aws", bucket="test-bucket", prefix="test-prefix")
    )
    return client


# Test extract_parameters function
def test_extract_parameters_with_ordered_params(mock_computation):
    """Tests extracting parameters"""
    result = extract_parameters(mock_computation)
    assert result == {"param1": "value1", "param2": "value2"}


class MockModelWithCopy:
    """Minimal stand-in for a pydantic model with copy support."""

    def __init__(self, data: dict[str, object]):
        """Initialize fields"""
        self._data = dict(data)

    def model_copy(self, update: dict[str, object] | None = None):
        """Returns mock model copy with updated data"""
        updated = dict(self._data)
        if update:
            updated.update(update)
        return MockModelWithCopy(updated)

    def model_dump(self):
        """Dumps model to dict"""
        return dict(self._data)


def test_construct_processing_record():
    """Updates process metadata with dispatch details."""

    process = MockModel(
        code=MockModel(
            input_data=[],
            parameters=MockModelWithCopy({"existing": "keep"}),
        ),
        notes="Existing note",
    )

    analysis_job = AnalysisDispatchModel(
        s3_location=["s3://test-bucket/test-data"],
        asset_name=["test-data"],
        docdb_record_id=["id1"],
        distributed_parameters={"value_threshold": 0.5},
        query='{"field": 1}',
    )

    result = update_analysis_process(
        process,
        analysis_job,
        extra_param="foo",
        dry_run=True,
    )

    assert result.code.input_data[-1].url == "s3://test-bucket/test-data"
    assert result.code.parameters.model_dump() == {
        "existing": "keep",
        "extra_param": "foo",
        "value_threshold": 0.5,
    }
    assert (
        result.notes
        == 'Existing note\nQuery used to retrieve data assets: {"field": 1}'
    )


def test_update_analysis_process_leaves_base_record_reusable():
    """Updates land on a copy so one base record can serve many jobs.

    Regression test: the base record used to be updated in place, so
    input_data accumulated and parameters carried over from job to job.
    """

    base = MockModel(
        code=MockModel(input_data=[], parameters=MockModelWithCopy({})),
        notes=None,
    )
    jobs = [
        AnalysisDispatchModel(
            s3_location=[f"s3://test-bucket/{name}"],
            asset_name=[name],
            docdb_record_id=[f"id-{name}"],
            distributed_parameters={name: True},
        )
        for name in ("first", "second")
    ]

    results = [update_analysis_process(base, job) for job in jobs]

    assert base.code.input_data == []
    assert base.code.parameters.model_dump() == {}
    assert [[asset.url for asset in result.code.input_data] for result in results] == [
        ["s3://test-bucket/first"],
        ["s3://test-bucket/second"],
    ]
    assert [result.code.parameters.model_dump() for result in results] == [
        {"first": True},
        {"second": True},
    ]


# Test _initialize_codeocean_client function
@patch.dict(
    os.environ,
    {"CODEOCEAN_DOMAIN": "test-domain", "CODEOCEAN_API_TOKEN": "test-token"},
)
def test_initialize_codeocean_client_success():
    """Tests initializing code ocean client"""
    with patch("analysis_pipeline_utils.metadata.CodeOcean") as mock_co:
        _initialize_codeocean_client()
        mock_co.assert_called_once_with(
            domain="https://test-domain", token="test-token"
        )


def test_initialize_codeocean_client_missing_env():
    """Tests initializing code ocean client env"""
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ValueError):
            _initialize_codeocean_client()


# Test _run_git_command function
@patch("subprocess.run")
def test_run_git_command_success(mock_run):
    """Tests run git command"""
    mock_run.return_value = Mock(returncode=0, stdout="test output\n")

    result = _run_git_command(["git", "test-command"])
    assert result == "test output"
    mock_run.assert_called_once()


# Test get_data_asset_url function
def test_get_data_asset_url_aws(mock_code_ocean_client):
    """Tests getting data asset url from aws"""
    result = get_data_asset_url(mock_code_ocean_client, "test-asset-id")
    assert result == "s3://test-bucket/test-prefix"


def test_get_data_asset_url_non_aws(mock_code_ocean_client):
    """Tests getting data asset url not from aws"""
    mock_code_ocean_client.data_assets.get_data_asset.return_value = Mock(
        source_bucket=Mock(origin="other")
    )
    with pytest.raises(ValueError):
        get_data_asset_url(mock_code_ocean_client, "test-asset-id")


# Test DocDB related functions
@patch("analysis_pipeline_utils.metadata.get_docdb_records")
def test_docdb_record_exists_true(mock_get_records):
    """Tests if docdb record exists - true."""
    mock_get_records.return_value = ["record"]

    assert docdb_record_exists(Mock()) is True
    mock_get_records.assert_called_once()


@patch("analysis_pipeline_utils.metadata.get_docdb_records")
def test_docdb_record_exists_false(mock_get_records):
    """Tests if docdb record exists - false."""
    mock_get_records.return_value = []

    assert docdb_record_exists(Mock()) is False
    mock_get_records.assert_called_once()


@patch("analysis_pipeline_utils.metadata.get_docdb_client")
@patch(
    "analysis_pipeline_utils.metadata.processing_prefix",
    return_value="fakehash",
)
def test_get_docdb_record_single(mock_prefix, mock_get_client):
    """Tests getting single docdb record"""
    mock_client = Mock()
    mock_client.retrieve_docdb_records.return_value = ["record"]
    mock_get_client.return_value = mock_client

    processing = Mock()
    result = get_docdb_records(processing)
    assert result == ["record"]


@patch("analysis_pipeline_utils.metadata.MetadataDbClient")
@patch("analysis_pipeline_utils.metadata.get_record_from_docdb")
def test_get_metadata_for_records_all_found(mock_get_record, mock_client_cls):
    """Tests fetching metadata when all records are found."""
    mock_client = Mock()
    mock_client_cls.return_value = mock_client

    mock_get_record.side_effect = [
        {"_id": "id1", "field": "value1"},
        {"_id": "id2", "field": "value2"},
    ]

    analysis_job = AnalysisDispatchModel(
        s3_location=[],
        asset_name=[],
        docdb_record_id=["id1", "id2"],
    )

    result = get_metadata_for_records(analysis_job)

    assert result == [
        {"_id": "id1", "field": "value1"},
        {"_id": "id2", "field": "value2"},
    ]
    assert mock_get_record.call_count == 2
    mock_client_cls.assert_called_once()


@patch("analysis_pipeline_utils.metadata.MetadataDbClient")
@patch("analysis_pipeline_utils.metadata.get_record_from_docdb")
def test_get_metadata_for_records_missing_record(mock_get_record, mock_client_cls):
    """Tests fetching metadata when some records are missing."""
    mock_client_cls.return_value = Mock()

    mock_get_record.side_effect = [
        {"_id": "id1", "field": "value1"},
        None,
    ]

    analysis_job = AnalysisDispatchModel(
        s3_location=[],
        asset_name=[],
        docdb_record_id=["id1", "id2"],
    )

    result = get_metadata_for_records(analysis_job)

    assert result == [{"_id": "id1", "field": "value1"}]
    assert mock_get_record.call_count == 2


@patch("analysis_pipeline_utils.metadata.MetadataDbClient")
@patch("analysis_pipeline_utils.metadata.get_record_from_docdb")
def test_get_metadata_for_records_none_found(mock_get_record, mock_client_cls):
    """Tests fetching metadata when no records are found."""
    mock_client_cls.return_value = Mock()
    mock_get_record.return_value = None

    analysis_job = AnalysisDispatchModel(
        s3_location=[],
        asset_name=[],
        docdb_record_id=["id1", "id2"],
    )

    result = get_metadata_for_records(analysis_job)

    assert result == []
    assert mock_get_record.call_count == 2


# Test release version lookup (see issue #48)
@patch("analysis_pipeline_utils.metadata._get_git_remote_url")
@patch("analysis_pipeline_utils.metadata._run_git_command")
def test_get_commits_since_release_converts_timestamp(mock_git, mock_url):
    """Unix timestamps from the Code Ocean API are converted for git."""
    mock_url.return_value = "https://example.com/capsule-1.git"
    mock_git.return_value = "abc123\ndef456"

    result = get_commits_since_release(Mock(slug="1"), release_time=1768017516)

    assert result == ["abc123", "def456"]
    log_command = mock_git.call_args_list[-1].args[0]
    since = next(a for a in log_command if a.startswith("--since="))
    assert since == f"--since={datetime.fromtimestamp(1768017516).isoformat()}"


@patch("analysis_pipeline_utils.metadata._get_git_remote_url")
@patch("analysis_pipeline_utils.metadata._run_git_command")
def test_get_commits_since_release_clone_args(mock_git, mock_url):
    """The clone omits --branch for HEAD and never passes --shallow-since."""
    mock_url.return_value = "https://example.com/capsule-1.git"
    mock_git.return_value = ""

    get_commits_since_release(Mock(slug="1"), release_time=1768017516)
    clone_command = mock_git.call_args_list[0].args[0]
    # 'HEAD' is not a branch name and would fail with "Remote branch not found"
    assert "--branch" not in clone_command
    # --shallow-since aborts the clone when no commits match the cutoff
    assert "--shallow-since" not in clone_command

    mock_git.reset_mock()
    get_commits_since_release(Mock(slug="1"), release_time=1768017516, branch="main")
    clone_command = mock_git.call_args_list[0].args[0]
    assert clone_command[clone_command.index("--branch") + 1] == "main"


@patch("analysis_pipeline_utils.metadata.get_latest_release")
@patch("analysis_pipeline_utils.metadata.get_commits_since_release")
def test_get_capsule_version_no_commits_since_release(mock_commits, mock_release):
    """No commits since release means the release version is used."""
    mock_release.return_value = {
        "major_version": 2,
        "minor_version": 0,
        "release_time": 1768017516,
    }
    mock_commits.return_value = []

    assert get_capsule_version_ignoring_patches(Mock()) == "2.0"


@patch("analysis_pipeline_utils.metadata.get_latest_release")
@patch("analysis_pipeline_utils.metadata.get_commits_since_release")
def test_get_capsule_version_holds_at_release(mock_commits, mock_release, caplog):
    """Commits after a release do not change the version, but do warn.

    The analysis owner decides what is a substantial new version by cutting a
    new release (see analysis-pipeline-utils#30).
    """
    mock_release.return_value = {
        "major_version": 2,
        "minor_version": 1,
        "release_time": 1768017516,
    }
    mock_commits.return_value = ["aaa", "bbb"]

    assert get_capsule_version_ignoring_patches(Mock()) == "2.1"
    assert "2 commit(s) since release 2.1" in caplog.text

    # commits listed as patches are not worth warning about
    caplog.clear()
    assert get_capsule_version_ignoring_patches(Mock(), patch_list="aaa,bbb") == "2.1"
    assert caplog.text == ""


@patch("analysis_pipeline_utils.metadata.get_latest_release")
def test_get_capsule_version_no_release(mock_release):
    """Without a release there is no version to use; caller falls back."""
    mock_release.return_value = None

    assert get_capsule_version_ignoring_patches(Mock()) is None


@patch("analysis_pipeline_utils.metadata.get_latest_release")
@patch("analysis_pipeline_utils.metadata.get_commits_since_release")
def test_get_capsule_version_survives_commit_lookup_failure(mock_commits, mock_release):
    """A failed commit listing must not change the recorded version."""
    mock_release.return_value = {
        "major_version": 2,
        "minor_version": 0,
        "release_time": 1768017516,
    }
    mock_commits.side_effect = RuntimeError("git exploded")

    assert get_capsule_version_ignoring_patches(Mock()) == "2.0"


@patch("analysis_pipeline_utils.metadata.get_capsule_releases")
def test_get_release_version_for_major(mock_releases):
    """A pinned component's int major version expands to '<major>.<minor>'."""
    mock_releases.return_value = [
        {"major_version": 1, "minor_version": 0, "release_time": 1},
        {"major_version": 2, "minor_version": 0, "release_time": 2},
        {"major_version": 2, "minor_version": 3, "release_time": 3},
    ]

    # latest minor wins within a major
    assert get_release_version_for_major(Mock(), 2) == "2.3"
    assert get_release_version_for_major(Mock(), 1) == "1.0"
    # unknown major degrades rather than failing
    assert get_release_version_for_major(Mock(), 9) == "9"
