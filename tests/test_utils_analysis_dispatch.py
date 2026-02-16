"""Tests for utils_analysis_dispatch helpers."""

import json
from types import SimpleNamespace as MockModel
from unittest.mock import patch

import pytest

from analysis_pipeline_utils.analysis_dispatch_model import (
    AnalysisDispatchModel,
)
from analysis_pipeline_utils.utils_analysis_dispatch import (
    check_task_parameters,
    get_asset_file_path_records,
    get_data_asset_records,
    expand_task_list,
    query_data_assets,
    read_asset_ids_from_csv,
    write_input_model_list,
)


@patch("analysis_pipeline_utils.utils_analysis_dispatch._docdb_api_client")
def test_query_data_assets_with_group(mock_docdb_client):
    """Builds expected aggregation pipeline when grouping is provided."""

    query = {"experiment_type": "behavior"}
    group_by = ["session"]

    mock_client = mock_docdb_client.return_value
    expected_response = [
        {
            "_id": ["sess1"],
            "s3_location": ["bucket/a"],
            "docdb_record_id": ["id1"],
            "group_metadata": {"session": "sess1"},
        }
    ]
    mock_client.aggregate_docdb_records.return_value = expected_response

    result = query_data_assets(query=query, group_by=group_by)

    mock_client.aggregate_docdb_records.assert_called_once_with(
        pipeline=[
            {"$match": query},
            {"$match": {"session": {"$ne": None}}},
            {
                "$group": {
                    "_id": ["$session"],
                    "s3_location": {"$push": "$location"},
                    "docdb_record_id": {"$push": "$_id"},
                    "session": {"$first": "$session"},
                }
            },
            {
                "$addFields": {
                    "group_metadata": {"session": "$session"}
                }
            },
            {
                "$project": {
                    "session": 0,
                }
            },
        ]
    )

    assert result == expected_response


@patch("analysis_pipeline_utils.utils_analysis_dispatch._docdb_api_client")
def test_query_data_assets_no_group(mock_docdb_client):
    """Falls back to projecting s3_location/docdb_record_id when not grouped."""

    mock_client = mock_docdb_client.return_value
    mock_client.aggregate_docdb_records.return_value = [
        {"s3_location": ["bucket/x"], "docdb_record_id": ["idx"]}
    ]

    result = query_data_assets(query={"a": 1}, group_by=None)

    mock_client.aggregate_docdb_records.assert_called_once_with(
        pipeline=[
            {"$match": {"a": 1}},
            {
                "$project": {
                    "s3_location": ["$location"],
                    "docdb_record_id": ["$_id"],
                }
            },
        ]
    )

    assert result == [{"s3_location": ["bucket/x"], "docdb_record_id": ["idx"]}]


@patch("analysis_pipeline_utils.utils_analysis_dispatch._docdb_api_client")
def test_query_data_assets_filter_latest(mock_docdb_client):
    """Includes latest-filter stages and requires filter_by."""

    mock_client = mock_docdb_client.return_value
    mock_client.aggregate_docdb_records.return_value = []

    query = {"status": "complete"}
    result = query_data_assets(
        query=query,
        filter_latest="created_at",
        filter_by=["session_id"],
    )

    assert result == []
    mock_client.aggregate_docdb_records.assert_called_once_with(
        pipeline=[
            {"$match": query},
            {"$match": {"session_id": {"$ne": None}}},
            {"$sort": {"created_at": -1}},
            {
                "$group": {
                    "_id": ["$session_id"],
                    "record": {"$first": "$$ROOT"},
                }
            },
            {"$replaceRoot": {"newRoot": "$record"}},
            {
                "$project": {
                    "s3_location": ["$location"],
                    "docdb_record_id": ["$_id"],
                }
            },
        ]
    )


def test_query_data_assets_filter_latest_requires_group():
    """Raises when filter_latest provided without filter_by."""

    with pytest.raises(ValueError):
        query_data_assets(query={}, filter_latest="created_at")


@patch("analysis_pipeline_utils.utils_analysis_dispatch._docdb_api_client")
def test_query_data_assets_unwind_and_group(mock_docdb_client):
    """Unwinds list fields before grouping and retains metadata."""

    mock_client = mock_docdb_client.return_value
    mock_client.aggregate_docdb_records.return_value = []

    query_data_assets(
        query={"status": "complete"},
        group_by=["metadata.animal_id"],
        unwind_list_fields=["tags"],
        drop_null_groups=False,
    )

    mock_client.aggregate_docdb_records.assert_called_once()
    pipeline = mock_client.aggregate_docdb_records.call_args.kwargs["pipeline"]
    assert pipeline[0] == {"$match": {"status": "complete"}}
    # drop_null_groups disabled so first unwind stage should appear directly
    assert pipeline[1] == {
        "$addFields": {
            "tags": {
                "$cond": {
                    "if": {"$isArray": "$tags"},
                    "then": "$tags",
                    "else": ["$tags"],
                }
            }
        }
    }
    assert pipeline[2] == {"$unwind": "$tags"}
    group_stage = pipeline[3]
    assert group_stage["$group"]["_id"] == ["$metadata.animal_id"]
    assert pipeline[-1] == {"$project": {"metadata__animal_id": 0}}


@patch("analysis_pipeline_utils.utils_analysis_dispatch.fs")
def test_get_asset_file_path_records_split_true(mock_fs):
    """Splits individual files into separate models when requested."""

    mock_fs.glob.return_value = [
        "bucket/key/file1.tif",
        "bucket/key/file2.tif",
    ]

    record = AnalysisDispatchModel(
        s3_location=["bucket/key"],
        docdb_record_id=["id1"],
    )

    results = get_asset_file_path_records(
        record, file_extension=".tif", split_files=True
    )

    assert len(results) == 2
    assert results[0].file_location == ["s3://bucket/key/file1.tif"]
    assert results[1].file_location == ["s3://bucket/key/file2.tif"]
    mock_fs.glob.assert_called_once_with("bucket/key/**/*" + ".tif")


@patch("analysis_pipeline_utils.utils_analysis_dispatch.fs")
def test_get_asset_file_path_records_split_false(mock_fs):
    """Keeps file list together when split_files is False."""

    mock_fs.glob.return_value = [
        "bucket/key/file1.tif",
        "bucket/key/file2.tif",
    ]

    record = AnalysisDispatchModel(
        s3_location=["bucket/key"],
        docdb_record_id=["id1"],
    )

    results = get_asset_file_path_records(
        record, file_extension=".tif", split_files=False
    )

    assert len(results) == 1
    assert results[0].file_location == [
        "s3://bucket/key/file1.tif",
        "s3://bucket/key/file2.tif",
    ]
    assert results[0].s3_location == ["bucket/key"]
    assert results[0].docdb_record_id == ["id1"]


@patch("analysis_pipeline_utils.utils_analysis_dispatch.fs")
def test_get_asset_file_path_records_no_files(mock_fs):
    """Skips assets with no matching files."""

    mock_fs.glob.return_value = []

    record = AnalysisDispatchModel(
        s3_location=["bucket/key"],
        docdb_record_id=["id1"],
    )

    results = get_asset_file_path_records(
        record, file_extension=".tif", split_files=False
    )

    assert results == []


@patch("analysis_pipeline_utils.utils_analysis_dispatch.fs")
def test_get_asset_file_path_records_split_multiple_assets_raises(mock_fs):
    """Cannot split when multiple grouped assets are present."""

    mock_fs.glob.return_value = ["bucket/key/file1.tif"]

    record = AnalysisDispatchModel(
        s3_location=["bucket/key", "bucket/key2"],
        docdb_record_id=["id1", "id2"],
    )

    with pytest.raises(ValueError):
        get_asset_file_path_records(record, file_extension=".tif", split_files=True)


@patch("analysis_pipeline_utils.utils_analysis_dispatch.get_asset_file_path_records")
def test_expand_task_list_with_parameters(mock_get_files):
    """Expands records when distributed parameters are provided."""

    base_record = AnalysisDispatchModel(
        s3_location=["bucket/key"],
        docdb_record_id=["id1"],
    )
    mock_get_files.return_value = [
        base_record.model_copy(update={"file_location": ["s3://bucket/key/f1"]})
    ]

    params = [{"p": 1}, {"p": 2}]

    result = expand_task_list(
        [base_record],
        file_extension=".tif",
        split_files=False,
        distributed_analysis_parameters=params,
    )
    result = list(result)

    assert len(result) == 2
    assert result[0].distributed_parameters == {"p": 1}
    assert result[1].distributed_parameters == {"p": 2}
    mock_get_files.assert_called_once_with(
        base_record, file_extension=".tif", split_files=False
    )


@patch("analysis_pipeline_utils.utils_analysis_dispatch.get_asset_file_path_records")
def test_expand_task_list_skips_empty_file_records(mock_get_files):
    """Does not emit models when no files are found."""

    base_record = AnalysisDispatchModel(
        s3_location=["bucket/key"],
        docdb_record_id=["id1"],
    )
    mock_get_files.return_value = []

    result = expand_task_list(
        [base_record],
        file_extension=".tif",
        split_files=False,
    )
    result = list(result)

    assert result == []


def test_expand_task_list_no_extension_no_parameters():
    """Returns records unchanged when no file discovery or parameters."""

    records = [
        AnalysisDispatchModel(
            s3_location=["bucket/a"],
            docdb_record_id=["id1"],
        ),
        AnalysisDispatchModel(
            s3_location=["bucket/b"],
            docdb_record_id=["id2"],
        ),
    ]

    result = expand_task_list(records)
    result = list(result)

    assert len(result) == 2
    assert result[0].s3_location == ["bucket/a"]
    assert result[0].docdb_record_id == ["id1"]
    assert result[0].file_location is None
    assert result[0].distributed_parameters is None
    assert result[1].s3_location == ["bucket/b"]
    assert result[1].docdb_record_id == ["id2"]


def test_read_asset_ids_from_csv_valid(tmp_path):
    """Reads valid asset IDs from CSV"""
    csv_path = tmp_path / "assets.csv"
    csv_path.write_text("asset_id,other\nid1,foo\nid2,bar\n")

    result = read_asset_ids_from_csv(csv_path)

    assert result == ["id1", "id2"]


def test_read_asset_ids_from_csv_ignores_empty_rows(tmp_path):
    """Ignores empty or whitespace-only asset_id values"""
    csv_path = tmp_path / "assets.csv"
    csv_path.write_text("asset_id\nid1\n\n   \nid2\n")

    result = read_asset_ids_from_csv(csv_path)

    assert result == ["id1", "id2"]


def test_read_asset_ids_from_csv_missing_column(tmp_path):
    """Raises if asset_id column is missing"""
    csv_path = tmp_path / "assets.csv"
    csv_path.write_text("wrong_col\nid1\n")

    with pytest.raises(ValueError, match="asset_id"):
        read_asset_ids_from_csv(csv_path)


@patch("analysis_pipeline_utils.utils_analysis_dispatch.query_data_assets")
def test_get_data_asset_records_from_csv(mock_query, tmp_path):
    """Reads asset IDs from CSV and queries DocDB."""

    csv_path = tmp_path / "assets.csv"
    csv_path.write_text("asset_id\nid1\nid2\n")

    mock_query.return_value = [
        {"s3_location": ["bucket/id1"], "docdb_record_id": ["doc1"]},
        {"s3_location": ["bucket/id2"], "docdb_record_id": ["doc2"]},
    ]

    records = get_data_asset_records(input_directory=tmp_path, use_data_asset_csv=True)

    mock_query.assert_called_once_with(
        query={"external_links.Code Ocean.0": {"$in": ["id1", "id2"]}},
    )

    assert len(records) == 2
    assert records[0].s3_location == ["bucket/id1"]
    assert records[0].docdb_record_id == ["doc1"]


def test_get_data_asset_records_csv_missing(tmp_path):
    """Raises when CSV is requested but missing."""

    with pytest.raises(FileNotFoundError):
        get_data_asset_records(input_directory=tmp_path, use_data_asset_csv=True)


@patch("analysis_pipeline_utils.utils_analysis_dispatch.query_data_assets")
def test_get_data_asset_records_docdb_query_path(mock_query, tmp_path):
    """Loads query from file path and queries DocDB."""

    query_path = tmp_path / "query.json"
    query_path.write_text(json.dumps({"a": 1}))

    mock_query.return_value = [
        {"s3_location": ["bucket/id1"], "docdb_record_id": ["doc1"]}
    ]

    records = get_data_asset_records(
        input_directory=tmp_path, docdb_query=str(query_path)
    )

    mock_query.assert_called_once_with(query={"a": 1})
    assert records[0].docdb_record_id == ["doc1"]


@patch("analysis_pipeline_utils.utils_analysis_dispatch.query_data_assets")
def test_get_data_asset_records_docdb_query_string(mock_query, tmp_path):
    """Loads query from JSON string and queries DocDB."""

    query_str = json.dumps({"b": 2})
    mock_query.return_value = [
        {"s3_location": ["bucket/id2"], "docdb_record_id": ["doc2"]}
    ]

    records = get_data_asset_records(input_directory=tmp_path, docdb_query=query_str)

    mock_query.assert_called_once_with(query={"b": 2})
    assert records[0].docdb_record_id == ["doc2"]


@patch("analysis_pipeline_utils.utils_analysis_dispatch.uuid.uuid4")
def test_write_input_model_list_groups_jobs(mock_uuid, tmp_path):
    """Batches tasks per job folder and respects dispatch limits."""

    mock_uuid.side_effect = ["id1", "id2", "id3", "id4"]
    models = [
        AnalysisDispatchModel(
            s3_location=[f"bucket/{i}"],
            docdb_record_id=[f"doc{i}"],
        )
        for i in range(4)
    ]

    write_input_model_list(
        iter(models),
        tmp_path,
        tasks_per_job=2,
        max_number_of_tasks_dispatched=3,
    )

    job0 = tmp_path / "0"
    job1 = tmp_path / "1"
    assert job0.exists()
    assert job1.exists()

    files_job0 = sorted(job0.glob("*.json"))
    files_job1 = sorted(job1.glob("*.json"))
    assert [f.name for f in files_job0] == ["id1.json", "id2.json"]
    assert [f.name for f in files_job1] == ["id3.json"]

    payload = json.loads(files_job0[0].read_text())
    assert payload["s3_location"] == ["bucket/0"]
    assert payload["docdb_record_id"] == ["doc0"]


def test_write_input_model_list_invalid_group_size(tmp_path):
    """tasks_per_job must be at least 1."""

    model = AnalysisDispatchModel(
        s3_location=["bucket/a"],
        docdb_record_id=["doc"],
    )

    with pytest.raises(ValueError):
        write_input_model_list(iter([model]), tmp_path, tasks_per_job=0)


@patch("analysis_pipeline_utils.utils_analysis_dispatch.docdb_record_exists")
@patch("analysis_pipeline_utils.utils_analysis_dispatch.construct_processing_record")
@patch("analysis_pipeline_utils.utils_analysis_dispatch.get_codeocean_process_metadata")
def test_check_task_parameters_skips_processed(
    mock_get_process,
    mock_construct,
    mock_exists,
):
    """Skips processed jobs and keeps unprocessed ones."""

    base_process = MockModel(code=MockModel())
    mock_get_process.return_value = base_process
    first_process = MockModel(code=MockModel(name="skip"))
    second_process = MockModel(code=MockModel(name="keep"))
    mock_construct.side_effect = [first_process, second_process]
    mock_exists.side_effect = [True, False]

    inputs = [
        AnalysisDispatchModel(s3_location=["bucket/a"], docdb_record_id=["doc1"]),
        AnalysisDispatchModel(s3_location=["bucket/b"], docdb_record_id=["doc2"]),
    ]

    results = list(
        check_task_parameters(iter(inputs), fixed_analysis_params={"x": 1})
    )

    assert len(results) == 1
    assert results[0].analysis_code is second_process.code
    mock_construct.assert_any_call(base_process, inputs[0], x=1)
    mock_construct.assert_any_call(base_process, inputs[1], x=1)
    assert mock_exists.call_count == 2


@patch("analysis_pipeline_utils.utils_analysis_dispatch.docdb_record_exists")
@patch("analysis_pipeline_utils.utils_analysis_dispatch.construct_processing_record")
@patch("analysis_pipeline_utils.utils_analysis_dispatch.get_codeocean_process_metadata")
def test_check_task_parameters_no_filter(mock_get_process, mock_construct, mock_exists):
    """Yields all jobs when filter_processed is False."""

    base_process = MockModel(code=MockModel())
    mock_get_process.return_value = base_process
    mock_construct.side_effect = [
        MockModel(code=MockModel(name="first")),
        MockModel(code=MockModel(name="second")),
    ]
    mock_exists.return_value = True

    inputs = [
        AnalysisDispatchModel(s3_location=["bucket/a"], docdb_record_id=["doc1"]),
        AnalysisDispatchModel(s3_location=["bucket/b"], docdb_record_id=["doc2"]),
    ]

    results = list(check_task_parameters(iter(inputs), filter_processed=False))

    assert len(results) == 2
    assert results[0].analysis_code.name == "first"
    assert results[1].analysis_code.name == "second"
    assert mock_exists.call_count == 0
