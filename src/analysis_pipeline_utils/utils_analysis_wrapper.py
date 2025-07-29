import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Any, ClassVar, Optional, Type, TypeVar, Union

from aind_data_schema.base import GenericModel
from pydantic import create_model
from pydantic_settings import BaseSettings, SettingsConfigDict

from analysis_pipeline_utils.analysis_dispatch_model import (
    AnalysisDispatchModel,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


T = TypeVar("T", bound=GenericModel)


def make_cli_model(model_cls: Type[T]) -> Type[BaseSettings]:
    """
    Create a CLI-ready subclass of the given analysis specification model.

    Parameters
    ----------
    model_cls : Type[T]
        The base analysis specification Pydantic model

    Returns
    -------
    Type[BaseSettings]
        A new Pydantic Settings model that can parse CLI args
    """
    optional_model = create_model(
        f"Partial{model_cls.__name__}",
        __base__=GenericModel,
        **{
            name: (Optional[field.annotation], None)
            for name, field in model_cls.model_fields.items()
        },
    )

    class CLIModel(BaseSettings, optional_model):  # type: ignore
        model_config: ClassVar[SettingsConfigDict] = {
            "cli_parse_args": True,
        }

    CLIModel.__name__ = f"{model_cls.__name__}CLI"
    return CLIModel


def _get_merged_analysis_parameters(
    fixed_parameters: dict[str, Any],
    cli_parameters: dict[str, Any],
    distributed_parameters: dict[str, Any],
) -> dict[str, Any]:
    """
    Merges the analysis parameters with priority for overriding same fields:
    fixed_parameters < cli_parameters < distributed_parameters

    Parameters
    ----------
    fixed_parameters: dict[str, Any]
        Fixed parameters that are stable through different analysis runs
    cli_parameters: dict[str, Any]
        Command line arguments for analysis parameters
    distributed_parameters: dict[str, Any]
        Parameters from dispatch that vary through different analysis runs

    Returns
    -------
    dict[str, Any]
    The merged parameters with priority
    """
    # Track where each key originates and where it gets overridden
    sources = defaultdict(lambda: "fixed_parameters")
    merged_parameters = {}

    # Start with fixed parameters
    for k, v in fixed_parameters.items():
        merged_parameters[k] = v

    # CLI overrides
    for k, v in cli_parameters.items():
        if k in merged_parameters and v is not None:
            logger.info(
                f"Parameter '{k}' overridden: "
                f"fixed_parameters -> cli_data "
                f"(value: {merged_parameters[k]} -> {v})"
            )
            merged_parameters[k] = v
            sources[k] = "cli_data"

    # Distributed overrides
    for k, v in distributed_parameters.items():
        if k in merged_parameters:
            logger.info(
                f"Parameter '{k}' overridden: {sources[k]} "
                f" -> distributed_parameters "
                f"(value: {merged_parameters[k]} -> {v})"
            )
        merged_parameters[k] = v
        sources[k] = "distributed_parameters"

    return merged_parameters


def get_analysis_model_parameters(
    analysis_dispatch_inputs: AnalysisDispatchModel,
    analysis_model: GenericModel,
    analysis_parameters_json_path: Union[Path, None] = None,
) -> dict[str, Any]:
    """
    Gets the analysis parameters for metadata and tracking

    Parameters
    ----------
    analysis_dispatch_inputs: AnalysisDispatchModel
        The input model with data information for analysis to be run on

    analysis_model: GenericModel
        The analysis model with user defined parameters

    analysis_parameters_json_path: Union[Path, None] = None
        The path to analysis_parameters.json file

    Returns
    -------
    dict[str, Any]
        The merged analysis parameters
    """
    fixed_parameters = {}
    if analysis_parameters_json_path.exists():
        with open(analysis_parameters_json_path, "r") as f:
            analysis_spec = json.load(f)
            if "fixed_parameters" in analysis_spec:
                fixed_parameters = analysis_spec["fixed_parameters"]
                logger.info("Found analysis specification json. Parsing it")
                logger.info(f"Found fixed parameters {fixed_parameters}")
    else:
        logger.info(
            "No analysis parameters json found. "
            "Defaulting to parameters passed in via input arguments"
        )

    if fixed_parameters:
        fixed_parameters_model = analysis_model.model_construct(
            **fixed_parameters
        ).model_dump()
    else:
        fixed_parameters_model = {}

    cli_model = make_cli_model(analysis_model)
    cli_parameters_model = cli_model().model_dump()
    logger.info(f"Command line parameters {cli_parameters_model}")
    if analysis_dispatch_inputs.distributed_parameters:
        distributed_parameters = (
            analysis_dispatch_inputs.distributed_parameters
        )
        logger.info(
            f"Found distributed parameters "
            f"from dispatch: {distributed_parameters} "
            "Will combine, with distributed parameters taking priority"
        )
    else:
        distributed_parameters = {}

    merged_parameters = _get_merged_analysis_parameters(
        fixed_parameters_model, cli_parameters_model, distributed_parameters
    )

    return merged_parameters
