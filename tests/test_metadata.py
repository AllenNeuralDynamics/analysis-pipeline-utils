"""
Tests functions for processing metadata
"""

import os
from unittest.mock import Mock, patch

import aind_data_schema.core.processing as ps
import pytest
from codeocean.computation import Computation, ComputationState, Param

from analysis_pipeline_utils.analysis_dispatch_model import (
    AnalysisDispatchModel,
)
from analysis_pipeline_utils.metadata import (
    _initialize_codeocean_client,
    _run_git_command,
    construct_processing_record,
    docdb_record_exists,
    extract_parameters,
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
def mock_pipeline_process():
    """
    Mocks the pipeline process
    """
    return [
        Mock(
            capsule_id="test_capsule_id",
            parameters=[
                {"name": "process1", "value": "value1"},
                {"name": "", "value": "value2"},
            ],
        )
    ]


@pytest.fixture
def mock_code_ocean_client():
    """
    Mocks the code ocean client
    """
    client = Mock()
    client.data_assets.get_data_asset.return_value = Mock(
        source_bucket=Mock(
            origin="aws", bucket="test-bucket", prefix="test-prefix"
        )
    )
    return client


# Test extract_parameters function
def test_extract_parameters_with_ordered_params(mock_computation):
    """Tests extracting parameters"""
    result = extract_parameters(mock_computation)
    assert result == {"param1": "value1", "param2": "value2"}


# Test construct_processing_record function
@patch("analysis_pipeline_utils.metadata.get_codeocean_process_metadata")
def test_construct_processing_record(mock_query):
    """Tests constructing processing record"""
    # Setup mock process
    mock_process = ps.DataProcess.model_construct()
    mock_process.code = ps.Code(
        url="https://github.com/test/repo",
        parameters={},
        name="test-repo",
        version="test-version",
        run_script="code/run",
        input_data=[],  # Start with empty input_data
    )
    mock_query.return_value = mock_process

    # Test data
    analysis_job = AnalysisDispatchModel(
        s3_location=["s3://test-bucket/test-data"], docdb_record_id=["id1"]
    )

    result = construct_processing_record(analysis_job)

    assert isinstance(result, ps.DataProcess)
    assert len(result.code.input_data) == 1
    assert result.code.input_data[0].url == "s3://test-bucket/test-data"


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
@patch("analysis_pipeline_utils.metadata.get_docdb_client")
@patch(
    "analysis_pipeline_utils.metadata.processing_prefix",
    return_value="fakehash",
)
def test_docdb_record_exists_true(mock_prefix, mock_get_client):
    """Tests if docdb record exists - true"""
    mock_client = Mock()
    mock_client.retrieve_docdb_records.return_value = ["record"]
    mock_get_client.return_value = mock_client

    processing = Mock()
    assert docdb_record_exists(processing) is True


@patch("analysis_pipeline_utils.metadata.get_docdb_client")
@patch(
    "analysis_pipeline_utils.metadata.processing_prefix",
    return_value="fakehash",
)
def test_docdb_record_exists_false(mock_prefix, mock_get_client):
    """Tests if docdb record exists - false"""
    mock_client = Mock()
    mock_client.retrieve_docdb_records.return_value = []
    mock_get_client.return_value = mock_client

    processing = Mock()
    assert docdb_record_exists(processing) is False


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


@patch("analysis_pipeline_utils.metadata.get_record_from_docdb")
@patch("analysis_pipeline_utils.metadata.get_docdb_client")
def test_get_metadata_for_records_all_found(mock_get_client, mock_get_record):
    """Tests fetching metadata when all records are found"""
    mock_get_client.return_value = Mock()

    # Mock returned records
    mock_get_record.side_effect = [
        {"_id": "id1", "field": "value1"},
        {"_id": "id2", "field": "value2"},
    ]

    analysis_job = AnalysisDispatchModel(
        s3_location=[],
        docdb_record_id=["id1", "id2"],
    )

    result = get_metadata_for_records(analysis_job)

    assert result == [
        {"_id": "id1", "field": "value1"},
        {"_id": "id2", "field": "value2"},
    ]
    assert mock_get_record.call_count == 2


@patch("analysis_pipeline_utils.metadata.get_record_from_docdb")
@patch("analysis_pipeline_utils.metadata.get_docdb_client")
def test_get_metadata_for_records_missing_record(
    mock_get_client, mock_get_record
):
    """Tests fetching metadata when some records are missing"""
    mock_get_client.return_value = Mock()

    # Second record is missing
    mock_get_record.side_effect = [
        {"_id": "id1", "field": "value1"},
        None,
    ]

    analysis_job = AnalysisDispatchModel(
        s3_location=[],
        docdb_record_id=["id1", "id2"],
    )

    result = get_metadata_for_records(analysis_job)

    # Only the found record should be returned
    assert result == [{"_id": "id1", "field": "value1"}]
    assert mock_get_record.call_count == 2


@patch("analysis_pipeline_utils.metadata.get_record_from_docdb")
@patch("analysis_pipeline_utils.metadata.get_docdb_client")
def test_get_metadata_for_records_none_found(mock_get_client, mock_get_record):
    """Tests fetching metadata when no records are found"""
    mock_get_client.return_value = Mock()
    mock_get_record.return_value = None

    analysis_job = AnalysisDispatchModel(
        s3_location=[],
        docdb_record_id=["id1", "id2"],
    )

    result = get_metadata_for_records(analysis_job)

    assert result == []
    assert mock_get_record.call_count == 2
