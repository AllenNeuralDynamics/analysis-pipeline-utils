import json
from pathlib import Path
from unittest.mock import mock_open, patch

from aind_data_schema.base import GenericModel
from pydantic import Field
from pydantic_settings import BaseSettings

from analysis_pipeline_utils.analysis_dispatch_model import (
    AnalysisDispatchModel,
)
from analysis_pipeline_utils.utils_analysis_wrapper import (
    get_analysis_model_parameters,
    make_cli_model,
)


class MockModel(GenericModel):
    """Create a mock analysis model for testing"""

    analysis_name: str = Field(
        ..., description="User-defined name for the analysis"
    )
    analysis_tag: str = Field(
        ...,
        description=(
            "User-defined tag to organize results "
            "for querying analysis output",
        ),
    )
    value_threshold: float = Field(..., description="Threshold on data")


def test_make_cli_model() -> None:
    """Test the creation of cli model object"""
    # Pass the class
    cli_model_class = make_cli_model(MockModel)
    assert issubclass(cli_model_class, BaseSettings)
    assert cli_model_class.__name__ == "MockModelCLI"

    # Instantiate the CLI model (empty because all fields optional)
    cli_model = cli_model_class()

    # Compare fields keys on the class
    model_fields = set(MockModel.model_fields.keys())
    cli_fields = set(cli_model_class.model_fields.keys())
    assert model_fields == cli_fields

    # check the CLI model instance fields exist (all optional, so default None)
    for field in model_fields:
        assert getattr(cli_model, field, "missing") is None


def test_get_analysis_model_parameters() -> None:
    """Tests getting analysis parameters"""
    analysis_dispatch_inputs = AnalysisDispatchModel(
        s3_location=["s3://path/to/bucket"],
        distributed_parameters={"value_threshold": 0.08},
    )
    params_dict = {
        "fixed_parameters": {
            "analysis_name": "a",
            "analysis_tag": "V1",
            "value_threshold": 0.5,
        },
    }
    mock_file_data = json.dumps(params_dict)

    fake_path = Path("/fake/path/analysis_parameters.json")

    with patch("builtins.open", mock_open(read_data=mock_file_data)):
        with patch.object(Path, "exists", return_value=True):
            merged = get_analysis_model_parameters(
                analysis_dispatch_inputs,
                MockModel,
                analysis_parameters_json_path=fake_path,
            )

    assert merged["analysis_name"] == "a"
    assert merged["value_threshold"] == 0.08  # from distributed
