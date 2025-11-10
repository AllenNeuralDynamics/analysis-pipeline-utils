"""
Tests functions from analysis dispatch utils
"""

from unittest.mock import patch

from analysis_pipeline_utils.analysis_dispatch_model import (
    AnalysisDispatchModel,
)
from analysis_pipeline_utils.utils_analysis_dispatch import (
    get_data_asset_paths_and_docdb_id_from_query,
    get_input_model_list,
    get_s3_and_docdb_input_information,
)


@patch("analysis_pipeline_utils.utils_analysis_dispatch.docdb_api_client")
def test_get_data_asset_ids_from_query(mock_docdb_client):
    """
    Tests getting data asset ids from docdb query with grouping
    """
    # Setup mock return value
    mock_docdb_client.aggregate_docdb_records.return_value = [
        {
            "_id": "group1",
            "asset_location": ["id1/path/to/bucket", "id2/path/to/bucket"],
            "docdb_id": "id1"
        },
        {"_id": "group2", "asset_location": ["id3/path/to/bucket"], "docdb_id": "id2"},
    ]

    query = {"experiment_type": "behavior"}
    group_by = "session"

    result_paths, result_ids = get_data_asset_paths_and_docdb_id_from_query(query, group_by)

    # Check that the mock was called correctly
    mock_docdb_client.aggregate_docdb_records.assert_called_once_with(
        pipeline=[
            {"$match": query},
            {
                "$group": {
                    "_id": "$session",
                    "asset_location": {"$push": "$location"},
                    "docdb_id": {"$push": "$_id"}
                }
            },
        ]
    )

    # Check the returned data
    assert result_paths == [
        ["id1/path/to/bucket", "id2/path/to/bucket"],
        ["id3/path/to/bucket"],
    ]
    assert result_ids == [
        "id1", "id2"
    ]


@patch("analysis_pipeline_utils.utils_analysis_dispatch.docdb_api_client")
def test_get_data_asset_ids_no_group(mock_docdb_client):
    """Tests getting data asset ids with no grouping"""
    mock_docdb_client.aggregate_docdb_records.return_value = [
        {"_id": "some_id", "asset_location": ["id4/path/to/bucket"], "docdb_id": "some_id"}
    ]

    result_path, result_id = get_data_asset_paths_and_docdb_id_from_query({"a": 1}, group_by=None)

    mock_docdb_client.aggregate_docdb_records.assert_called_once_with(
        pipeline=[
            {"$match": {"a": 1}},
            {
                "$group": {
                    "_id": "$_id",
                    "asset_location": {"$push": "$location"},
                    "docdb_id": {"$push": "$_id"}

                }
            },
        ]
    )

    assert result_path == [["id4/path/to/bucket"]]
    assert result_id == ["some_id"]


@patch("s3fs.S3FileSystem")
def test_get_s3_input_information_split_true(mock_s3fs):
    """Tests getting s3 information with splitting files"""
    mock_s3fs.glob.return_value = [
        "bucket/key/file1.tif",
        "bucket/key/file2.tif",
    ]
    mock_s3fs.return_value = mock_s3fs

    s3_paths, s3_file_paths, docdb_ids = get_s3_and_docdb_input_information(
        data_asset_paths=["bucket/key"],
        docdb_record_ids=["id1"],
        file_extension=".tif",
        split_files=True,
    )

    assert s3_paths == ["bucket/key"]
    assert s3_file_paths == [
        "s3://bucket/key/file1.tif",
        "s3://bucket/key/file2.tif",
    ]
    assert docdb_ids == ["id1"]
    mock_s3fs.glob.assert_called_once_with("bucket/key/**/*.tif")


@patch("s3fs.S3FileSystem")
def test_get_s3_input_information_split_false(mock_s3fs):
    """Tests getting s3 information without splitting files"""
    mock_s3fs.glob.return_value = [
        "bucket/key/file1.tif",
        "bucket/key/file2.tif",
    ]
    mock_s3fs.return_value = mock_s3fs

    s3_paths, s3_file_paths, docdb_ids = get_s3_and_docdb_input_information(
        data_asset_paths=["bucket/key"],
        docdb_record_ids=["id1"],
        file_extension=".tif",
        split_files=False,
    )

    assert s3_paths == ["bucket/key"]
    assert s3_file_paths == [
        ["s3://bucket/key/file1.tif", "s3://bucket/key/file2.tif"]
    ]
    assert docdb_ids == ["id1"]
    mock_s3fs.glob.assert_called_once_with("bucket/key/**/*.tif")


@patch("s3fs.S3FileSystem")
def test_get_s3_input_information_no_files(mock_s3fs):
    """Tests getting s3 information with no files"""
    mock_s3fs.glob.return_value = []
    mock_s3fs.return_value = mock_s3fs

    s3_paths, s3_file_paths, docdb_ids = get_s3_and_docdb_input_information(
        data_asset_paths=["bucket/key"],
        docdb_record_ids=["id1"],
    )

    assert s3_paths == ["bucket/key"]
    assert docdb_ids == ["id1"]
    assert not s3_file_paths


@patch("s3fs.S3FileSystem")
def test_get_s3_input_information_no_glob(mock_s3fs):
    """Tests getting s3 information with no files found"""
    mock_s3fs.glob.return_value = []
    mock_s3fs.return_value = mock_s3fs

    s3_paths, s3_file_paths, docdb_ids = get_s3_and_docdb_input_information(
        data_asset_paths=["bucket/key"], docdb_record_ids=["id1"], file_extension=".nwb"
    )

    assert not s3_paths
    assert not docdb_ids
    assert not s3_file_paths


@patch(
    "analysis_pipeline_utils.utils_analysis_dispatch.get_s3_and_docdb_input_information"
)
def test_flat_input_no_parameters(mock_get_s3_info):
    """Tests getting input model with flat list and
    no distributed parameters
    """
    mock_get_s3_info.return_value = (
        ["s3/bucket1", "s3/bucket2"],
        ["s3://bucket1/file1.tif", "s3://bucket2/file2.tif"],
        ["id1", "id2"]
        
    )

    input_paths = ["s3/bucket1", "s3/bucket2"]
    input_docdb_ids = ["id1", "id2"]
    result = get_input_model_list(
        input_paths, input_docdb_ids, file_extension=".tif", split_files=True
    )

    assert len(result) == 2
    assert isinstance(result[0], AnalysisDispatchModel)
    assert result[0].s3_location == ["s3/bucket1"]
    assert result[0].file_location == ["s3://bucket1/file1.tif"]
    assert result[0].distributed_parameters is None
    assert result[0].docdb_record_id == ["id1"]
    assert result[1].docdb_record_id == ["id2"]


@patch(
    "analysis_pipeline_utils.utils_analysis_dispatch.get_s3_and_docdb_input_information"
)
def test_flat_input_with_parameters(mock_get_s3_info):
    """Test getting input model with flat
    list and distributed parameters"""
    mock_get_s3_info.return_value = (
        ["s3/bucket1", "s3/bucket2"],
        ["s3://bucket1/file1.tif", "s3://bucket2/file2.tif"],
        ["id1", "id2"]
    )

    input_paths = ["s3/bucket1", "s3/bucket2"]
    input_docdb_ids = ["id1", "id2"]
    parameters = [{"param": 1}, {"param": 2}]

    result = get_input_model_list(
        input_paths,
        input_docdb_ids,
        file_extension=".tif",
        split_files=True,
        distributed_analysis_parameters=parameters,
    )

    assert len(result) == 4  # 2 assets × 2 param sets
    assert result[1].distributed_parameters == {"param": 2}
    assert result[2].s3_location == ["s3/bucket2"]

    assert result[0].docdb_record_id == ["id1"]
    assert result[1].docdb_record_id == ["id1"]
    assert result[2].docdb_record_id == ["id2"]
    assert result[3].docdb_record_id == ["id2"]


@patch(
    "analysis_pipeline_utils.utils_analysis_dispatch.get_s3_and_docdb_input_information"
)
def test_nested_input_no_parameters(mock_get_s3_info):
    """
    Tests getting input model with grouped assets
    and no distributed parameters
    """
    mock_get_s3_info.return_value = (
        ["s3://bucket1", "s3://bucket2"],
        ["s3://bucket1/file1.tif", "s3://bucket1/file2.tif"],
        ["id1", "id2"]
    )

    input_paths = [["s3/bucket1", "s3/bucket2"]]  # one group of assets
    input_ids = [["id1", "id2"]]
    result = get_input_model_list(
        input_paths, input_ids, file_extension=".tif", split_files=False
    )

    assert len(result) == 1  # One group
    assert result[0].s3_location == ["s3://bucket1", "s3://bucket2"]
    assert result[0].file_location == [
        "s3://bucket1/file1.tif",
        "s3://bucket1/file2.tif",
    ]
    assert result[0].docdb_record_id == ["id1", "id2"]


@patch(
    "analysis_pipeline_utils.utils_analysis_dispatch.get_s3_and_docdb_input_information"
)
def test_nested_input_with_parameters(mock_get_s3_info):
    """
    Tests getting input model with grouped assets
    and distributed parameters
    """
    mock_get_s3_info.return_value = (
        ["s3://bucket1", "s3://bucket2"],
        ["s3://bucket1/file1.tif", "s3://bucket1/file2.tif"],
        ["id1", "id2"]
    )

    params = [{"param": "a"}, {"param": "b"}]
    input_paths = [["s3://bucket1", "s3://bucket2"]]  # one group of assets
    input_ids = [["id1", "id2"]]
    result = get_input_model_list(
        input_paths,
        input_ids,
        file_extension=".tif",
        split_files=False,
        distributed_analysis_parameters=params,
    )

    assert len(result) == 2  # # one group × 2 param sets
    assert result[0].s3_location == ["s3://bucket1", "s3://bucket2"]
    assert result[0].distributed_parameters == {"param": "a"}
    assert result[0].file_location == [
        "s3://bucket1/file1.tif",
        "s3://bucket1/file2.tif",
    ]
    assert result[0].docdb_record_id == ["id1", "id2"]

    assert result[1].s3_location == ["s3://bucket1", "s3://bucket2"]
    assert result[1].docdb_record_id == ["id1", "id2"]
    assert result[1].distributed_parameters == {"param": "b"}
