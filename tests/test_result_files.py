"""
Tests functions for interacting
with results from analysis
"""

import hashlib
from unittest.mock import MagicMock, patch

import aind_data_schema.core.processing as ps
import pytest
from aind_data_schema.core.metadata import Metadata

from analysis_pipeline_utils.result_files import (
    _processing_prefix,
    copy_results_to_s3,
    create_results_metadata,
)


@pytest.fixture
def mock_process():
    """Create a mock DataProcess object for testing."""
    process = ps.DataProcess(
        experimenters=["Test User"],
        process_type=ps.ProcessName.ANALYSIS,
        stage=ps.ProcessStage.ANALYSIS,
        start_date_time="2023-10-01T00:00:00Z",
        end_date_time="2023-10-01T01:00:00Z",
        code=ps.Code(
            url="https://github.com/test/repo",
            parameters={"param1": "value1"},
            name="test-repo",
            version="test-version",
            run_script="code/run",
        ),
    )
    return process


@pytest.fixture
def mock_metadata():
    """
    Mock metadata
    """
    return Metadata(name="test", location="s3://mock-bucket/mock-path")


def test_copy_results_to_s3_success(mock_metadata):
    """Tests copying results with success"""
    with patch("fsspec.filesystem") as mock_filesystem:
        mock_fs = MagicMock()
        mock_fs.exists.return_value = False
        mock_filesystem.return_value = mock_fs

        copy_results_to_s3(metadata=mock_metadata, results_path="/results")

        mock_filesystem.assert_called_once_with("s3")
        mock_fs.exists.assert_called_once_with(mock_metadata.location)
        mock_fs.put.assert_called_once_with(
            "/results", mock_metadata.location, recursive=True
        )


def test_copy_results_to_s3_already_exists(mock_metadata):
    """Tests copying results with failure"""
    with patch("fsspec.filesystem") as mock_filesystem:
        mock_fs = MagicMock()
        mock_fs.exists.return_value = True
        mock_filesystem.return_value = mock_fs

        with pytest.raises(
            Exception,
            match="S3 path s3://mock-bucket/mock-path already exists.",
        ):
            copy_results_to_s3(metadata=mock_metadata, results_path="/results")


def test_create_results_metadata(mock_process):
    """Test that create_results_metadata returns a valid Metadata object."""
    s3_bucket = "test-bucket"

    result = create_results_metadata(mock_process, s3_bucket)

    # Check that the result is a Metadata object
    assert isinstance(result, Metadata)

    # Check that the processing field contains our process
    assert len(result.processing.data_processes) == 1
    assert result.processing.data_processes[0] == mock_process

    # Check that the name matches the expected prefix
    expected_prefix = _processing_prefix(mock_process)
    assert result.name == expected_prefix

    # Check that the location is correctly constructed
    assert result.location == f"s3://{s3_bucket}/{expected_prefix}"


def test_processing_prefix_consistency(mock_process):
    """Test that _processing_prefix returns
    consistent results for the same input."""
    prefix1 = _processing_prefix(mock_process)
    prefix2 = _processing_prefix(mock_process)

    assert prefix1 == prefix2
    assert len(prefix1) == 64  # SHA-256 hash length in hex


def test_processing_prefix_uniqueness():
    """Test that _processing_prefix returns
    different results for different inputs."""
    process1 = ps.DataProcess.model_construct()
    process1.code = ps.Code(
        url="https://github.com/test/repo", name="process1", version="1.0"
    )

    process2 = ps.DataProcess.model_construct()
    process2.code = ps.Code(
        url="https://github.com/test/repo", name="process2", version="1.0"
    )

    prefix1 = _processing_prefix(process1)
    prefix2 = _processing_prefix(process2)

    assert prefix1 != prefix2


def test_processing_prefix_implementation():
    """Test the actual implementation of _processing_prefix."""
    process = ps.DataProcess.model_construct()
    process_str_json = process.model_dump_json().encode("utf-8")
    expected_hash = hashlib.sha256(process_str_json).hexdigest()

    assert _processing_prefix(process) == expected_hash
