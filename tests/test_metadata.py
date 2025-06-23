import pytest
from unittest.mock import Mock, patch
from datetime import datetime
import os
import aind_data_schema.core.processing as ps
from aind_data_schema.components.identifiers import DataAsset
from codeocean.computation import Computation, Param, ComputationState
from aind_analysis_results.metadata import (
    extract_parameters,
    _extract_parameters,
    construct_processing_record,
    _initialize_codeocean_client,
    get_code_metadata_from_git,
    get_data_asset_url,
    _run_git_command,
    docdb_record_exists,
    get_docdb_record,
)

# Fixtures for common test data
@pytest.fixture
def mock_computation():
    computation = Computation(
        id="test_computation_id",
        parameters=[
            Param(name="param1", value="value1"),
            Param(name="param2", value="value2")
        ],
        created=1622764800,
        run_time=3600,
        name="test_capsule",
        state=ComputationState.Running
    )
    return computation

@pytest.fixture
def mock_pipeline_process():
    return [
        Mock(
            capsule_id="test_capsule_id",
            parameters=[
                {"name": "process1", "value": "value1"},
                {"name": "", "value": "value2"}
            ]
        )
    ]

@pytest.fixture
def mock_code_ocean_client():
    client = Mock()
    client.data_assets.get_data_asset.return_value = Mock(
        source_bucket=Mock(
            origin="aws",
            bucket="test-bucket",
            prefix="test-prefix"
        )
    )
    return client

# Test extract_parameters function
def test_extract_parameters_with_ordered_params(mock_computation):
    result = extract_parameters(mock_computation)
    assert result == {
        "param1": "value1",
        "param2": "value2"
    }

def test_extract_parameters_with_capsule_id(mock_computation, mock_pipeline_process):
    mock_computation.processes = mock_pipeline_process
    result = extract_parameters(mock_computation, "test_capsule_id")
    assert result == {
        "param_0": "param1",
        "param_1": "param2",
        "named1": "value1",
        "named2": "value2",
        "process1": "value1",
        "param_1": "value2"  # Note: This would override the earlier param_1
    }

# Test _get_capsule_parameters function
def test_get_capsule_parameters_matching_capsule(mock_pipeline_process):
    result = _extract_parameters(mock_pipeline_process, "test_capsule_id")
    assert result == {
        "process1": "value1",
        "param_1": "value2"
    }

def test_get_capsule_parameters_no_match():
    processes = [Mock(capsule_id="different_id", parameters=[])]
    result = _extract_parameters(processes, "test_capsule_id")
    assert result == {}

# Test construct_processing_record function
@patch('aind_analysis_results.metadata.query_code_ocean_metadata')
def test_construct_processing_record(mock_query):
    # Setup mock process
    mock_process = ps.DataProcess.model_construct()
    mock_process.code = ps.Code(
        url="https://github.com/test/repo",
        parameters={},
        name="test-repo",
        version="test-version",
        run_script="code/run",
        input_data=[]  # Start with empty input_data
    )
    mock_query.return_value = mock_process

    # Test data
    analysis_job = {
        "s3_location": "s3://test-bucket/test-data",
        "parameters": {"param1": "value1"}
    }

    result = construct_processing_record(analysis_job)
    
    assert isinstance(result, ps.DataProcess)
    assert len(result.code.input_data) == 1
    assert result.code.input_data[0].url == "s3://test-bucket/test-data"
    assert result.code.parameters.model_dump() == {"param1": "value1"}

# Test _initialize_codeocean_client function
@patch.dict(os.environ, {
    "CODEOCEAN_DOMAIN": "test-domain",
    "CODEOCEAN_API_TOKEN": "test-token"
})
def test_initialize_codeocean_client_success():
    with patch('aind_analysis_results.metadata.CodeOcean') as mock_co:
        client = _initialize_codeocean_client()
        mock_co.assert_called_once_with(domain="test-domain", token="test-token")

def test_initialize_codeocean_client_missing_env():
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ValueError):
            _initialize_codeocean_client()

# Test get_code_metadata_from_git function
@patch('aind_analysis_results.metadata._run_git_command')
def test_get_code_metadata_from_git(mock_run_git):
    mock_run_git.side_effect = [
        "https://github.com/org/test-repo.git",  # remote URL
        "abc123"  # commit hash
    ]
    
    result = get_code_metadata_from_git()
    
    assert isinstance(result, ps.Code)
    assert result.url == "https://github.com/org/test-repo.git"
    assert result.version == "abc123"
    assert result.name == "test-repo"

# Test _run_git_command function
@patch('subprocess.run')
def test_run_git_command_success(mock_run):
    mock_run.return_value = Mock(
        returncode=0,
        stdout="test output\n"
    )
    
    result = _run_git_command(["git", "test-command"])
    assert result == "test output"
    mock_run.assert_called_once()

@patch('subprocess.run')
def test_run_git_command_failure(mock_run):
    mock_run.return_value = Mock(returncode=1, stdout="")  # Non-zero return code indicates failure
    
    result = _run_git_command(["git", "test-command"], default="failed")
    assert result == "failed"

# Test get_data_asset_url function
def test_get_data_asset_url_aws(mock_code_ocean_client):
    result = get_data_asset_url(mock_code_ocean_client, "test-asset-id")
    assert result == "s3://test-bucket/test-prefix"

def test_get_data_asset_url_non_aws(mock_code_ocean_client):
    mock_code_ocean_client.data_assets.get_data_asset.return_value = Mock(
        source_bucket=Mock(origin="other")
    )
    with pytest.raises(ValueError):
        get_data_asset_url(mock_code_ocean_client, "test-asset-id")

# Test DocDB related functions
@patch('aind_analysis_results.metadata.get_docdb_client')
def test_docdb_record_exists_true(mock_get_client):
    mock_client = Mock()
    mock_client.retrieve_docdb_records.return_value = ["record"]
    mock_get_client.return_value = mock_client
    
    processing = Mock()
    assert docdb_record_exists(processing) is True

@patch('aind_analysis_results.metadata.get_docdb_client')
def test_docdb_record_exists_false(mock_get_client):
    mock_client = Mock()
    mock_client.retrieve_docdb_records.return_value = []
    mock_get_client.return_value = mock_client
    
    processing = Mock()
    assert docdb_record_exists(processing) is False

@patch('aind_analysis_results.metadata.get_docdb_client')
def test_get_docdb_record_single(mock_get_client):
    mock_client = Mock()
    mock_client.retrieve_docdb_records.return_value = ["record"]
    mock_get_client.return_value = mock_client
    
    processing = Mock()
    result = get_docdb_record(processing)
    assert result == "record"

@patch('aind_analysis_results.metadata.get_docdb_client')
def test_get_docdb_record_multiple(mock_get_client):
    mock_client = Mock()
    mock_client.retrieve_docdb_records.return_value = ["record1", "record2"]
    mock_get_client.return_value = mock_client
    
    processing = Mock()
    with pytest.raises(ValueError):
        get_docdb_record(processing)
