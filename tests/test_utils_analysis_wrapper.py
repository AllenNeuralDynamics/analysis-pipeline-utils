"""Tests for the analysis wrapper helpers."""

from types import SimpleNamespace as MockModel
from unittest.mock import MagicMock, patch

from aind_data_schema.base import GenericModel
from pydantic import Field
from pydantic_settings import BaseSettings

from analysis_pipeline_utils.analysis_dispatch_model import (
    AnalysisDispatchModel,
)
from analysis_pipeline_utils.utils_analysis_wrapper import (
    make_cli_model_class,
    run_analysis_jobs,
)


class ExampleInput(GenericModel):
    """Create a mock analysis model for testing."""

    analysis_name: str = Field(..., description="User-defined name for the analysis")
    analysis_tag: str = Field(..., description="User-defined tag for querying outputs")
    value_threshold: float = Field(..., description="Threshold on data")


class ExampleOutput(GenericModel):
    """Simple output schema for run_analysis_jobs tests."""

    status: str = Field(..., description="Run status")


def test_make_cli_model_class_defaults() -> None:
    """CLI model wraps analysis model fields and helper options."""

    cli_model_class = make_cli_model_class(ExampleInput)

    assert issubclass(cli_model_class, BaseSettings)
    assert cli_model_class.__name__ == "ExampleInputCLI"

    cli_model = cli_model_class.model_construct()
    model_fields = set(ExampleInput.model_fields.keys())

    for field in model_fields:
        assert getattr(cli_model, field) is None

    assert cli_model.dry_run == 1
    assert cli_model.input_directory.as_posix() == "/data"


def test_run_analysis_jobs_executes_new_job(tmp_path):
    """run_analysis_jobs executes pipeline for unprocessed jobs."""

    job_dir = tmp_path / "job_dict"
    job_dir.mkdir()
    dispatch_model = AnalysisDispatchModel(
        s3_location=["s3://bucket"],
        asset_name=["asset1"],
        docdb_record_id=["doc1"],
    )
    job_path = job_dir / "job.json"
    job_path.write_text(dispatch_model.model_dump_json())

    cli_params = {"override": "value"}
    fake_cli_args = MockModel(
        dry_run=0,
        input_directory=tmp_path,
        model_dump=lambda exclude_unset=True: cli_params,
    )

    dummy_params = MockModel(
        model_dump=lambda: {
            "analysis_name": "Test",
            "analysis_tag": "v1",
            "value_threshold": 5.0,
        },
        model_dump_json=lambda: '{"analysis_name":"Test","analysis_tag":"v1","value_threshold":5.0}',  # noqa: E501
    )
    processing = MockModel(
        code=MockModel(parameters=dummy_params),
        model_dump_json=lambda *args, **kwargs: "{}",
    )
    base_process = MockModel(code=MockModel(parameters=None))

    mock_run = MagicMock(return_value={"status": "ok"})

    with (
        patch(
            "analysis_pipeline_utils.utils_analysis_wrapper.make_cli_model_class",
            return_value=MagicMock(return_value=fake_cli_args),
        ) as mock_cli,
        patch(
            "analysis_pipeline_utils.utils_analysis_wrapper.get_codeocean_process_metadata",  # noqa: E501
            return_value=base_process,
        ) as mock_get_process,
        patch(
            "analysis_pipeline_utils.utils_analysis_wrapper.construct_processing_record",  # noqa: E501
            return_value=processing,
        ) as mock_construct,
        patch(
            "analysis_pipeline_utils.utils_analysis_wrapper.docdb_record_exists",
            return_value=False,
        ) as mock_exists,
        patch(
            "analysis_pipeline_utils.utils_analysis_wrapper.write_results_and_metadata"
        ) as mock_write,
    ):
        run_analysis_jobs(ExampleInput, ExampleOutput, mock_run)

    mock_cli.assert_called_once_with(ExampleInput)
    mock_get_process.assert_called_once()
    mock_construct.assert_called_once()

    asserted_dispatch = mock_run.call_args.args[0]
    asserted_params = mock_run.call_args.args[1]
    assert isinstance(asserted_dispatch, AnalysisDispatchModel)
    assert isinstance(asserted_params, ExampleInput)
    assert asserted_params.value_threshold == 5.0

    mock_exists.assert_called_once()
    mock_write.assert_called_once_with(processing, dry_run=False)
    assert isinstance(processing.output_parameters, ExampleOutput)
    assert processing.output_parameters.status == "ok"


def test_run_analysis_jobs_skips_processed_job(tmp_path):
    """Already processed jobs are skipped and skip marker created."""

    job_dir = tmp_path / "job_dict"
    job_dir.mkdir()
    dispatch_model = AnalysisDispatchModel(
        s3_location=["s3://bucket"],
        asset_name=["asset1"],
        docdb_record_id=["doc1"],
    )
    job_path = job_dir / "job.json"
    job_path.write_text(dispatch_model.model_dump_json())

    fake_cli_args = MockModel(
        dry_run=1,
        input_directory=tmp_path,
        model_dump=lambda exclude_unset=True: {},
    )

    processing = MockModel(
        code=MockModel(
            parameters=MockModel(
                model_dump=lambda: {
                    "analysis_name": "Test",
                    "analysis_tag": "v1",
                    "value_threshold": 1.0,
                },
                model_dump_json=lambda: '{"analysis_name":"Test","analysis_tag":"v1","value_threshold":1.0}',  # noqa: E501
            )
        ),
        model_dump_json=lambda *args, **kwargs: "{}",
    )
    base_process = MockModel(code=MockModel(parameters=None))

    with (
        patch(
            "analysis_pipeline_utils.utils_analysis_wrapper.make_cli_model_class",
            return_value=MagicMock(return_value=fake_cli_args),
        ),
        patch(
            "analysis_pipeline_utils.utils_analysis_wrapper.get_codeocean_process_metadata",  # noqa: E501
            return_value=base_process,
        ),
        patch(
            "analysis_pipeline_utils.utils_analysis_wrapper.construct_processing_record",  # noqa: E501
            return_value=processing,
        ),
        patch(
            "analysis_pipeline_utils.utils_analysis_wrapper.docdb_record_exists",
            return_value=True,
        ),
        patch("analysis_pipeline_utils.utils_analysis_wrapper.os.mknod") as mock_mknod,
        patch(
            "analysis_pipeline_utils.utils_analysis_wrapper.write_results_and_metadata"
        ) as mock_write,
        patch("analysis_pipeline_utils.utils_analysis_wrapper.GenericModel"),
    ):
        run_analysis_jobs(ExampleInput, ExampleOutput, MagicMock())

    mock_write.assert_not_called()
    marker_name = f"/results/skip_{job_path.stem}"
    mock_mknod.assert_called_once_with(marker_name)
