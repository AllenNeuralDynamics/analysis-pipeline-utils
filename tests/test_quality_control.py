"""Tests functions for creating quality control records."""

from aind_data_schema.base import GenericModel
from aind_data_schema.core.quality_control import Stage, Status
from aind_data_schema_models.modalities import Modality
from pydantic import Field

from analysis_pipeline_utils.quality_control import (
    output_parameters_to_quality_control,
)


class MockOutputParameters(GenericModel):
    """Output parameters model for quality control tests"""

    unit_count: int = Field(..., description="Number of units")
    firing_rates: list[float] = Field(...)


def make_record(name, modalities):
    """Make a minimal metadata record for the given modalities"""
    return {
        "name": name,
        "data_description": {
            "modalities": [
                modality.model_dump(mode="json") for modality in modalities
            ]
        },
    }


def test_output_parameters_to_quality_control():
    """Each output parameter becomes a passing analysis metric."""
    output_parameters = MockOutputParameters(
        unit_count=3, firing_rates=[1.5, 2.0], extra_metric="extra"
    )

    qc = output_parameters_to_quality_control(
        output_parameters,
        [make_record("asset_1", [Modality.ECEPHYS, Modality.BEHAVIOR])],
    )

    assert [metric.name for metric in qc.metrics] == [
        "unit_count",
        "firing_rates",
        "extra_metric",
    ]
    assert [metric.value for metric in qc.metrics] == [3, [1.5, 2.0], "extra"]
    assert [metric.description for metric in qc.metrics] == [
        "Number of units",
        None,
        None,
    ]
    for metric in qc.metrics:
        assert metric.modality == Modality.ECEPHYS
        assert metric.stage == Stage.ANALYSIS
        assert metric.status.status == Status.PASS
        assert metric.status.evaluator == "Automated"
        assert metric.evaluated_assets is None
    assert qc.default_grouping == []
    assert qc.evaluate_status() == Status.PASS


def test_output_parameters_to_quality_control_multi_asset():
    """Multiple assets still make analysis metrics with the given status,
    and tags set the grouping."""
    output_parameters = MockOutputParameters(unit_count=3, firing_rates=[])

    qc = output_parameters_to_quality_control(
        output_parameters,
        [make_record("asset_1", [Modality.POPHYS]), make_record("asset_2", [])],
        evaluator="Test evaluator",
        status=Status.PENDING,
        tags={"analysis": "test_analysis"},
    )

    for metric in qc.metrics:
        assert metric.status.status == Status.PENDING
        assert metric.modality == Modality.POPHYS
        assert metric.stage == Stage.ANALYSIS
        assert metric.evaluated_assets is None
        assert metric.tags == {"analysis": "test_analysis"}
        assert metric.status.evaluator == "Test evaluator"
    assert qc.default_grouping == ["analysis"]
    assert qc.evaluate_status() == Status.PENDING
