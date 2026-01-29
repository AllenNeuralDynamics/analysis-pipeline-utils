"""
Functions that are called
in the analysis wrapper
"""

import json
import logging
from collections import defaultdict
import os
from pathlib import Path
from typing import Any, Callable, ClassVar, List, Optional, Tuple, Type, TypeVar, Union

from aind_data_schema.base import GenericModel
from analysis_pipeline_utils.metadata import construct_processing_record
from pydantic import Field, create_model
from pydantic_settings import BaseSettings, SettingsConfigDict

from analysis_pipeline_utils.analysis_dispatch_model import AnalysisDispatchModel
from analysis_pipeline_utils.metadata import write_results_and_metadata

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


T = TypeVar("T", bound=GenericModel)


def make_cli_model_class(model_cls: Type[T]) -> Type[BaseSettings]:
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
        """
        Class for pydantic command line model
        """

        dry_run: int = Field(
            default=1,
            description="Run without posting results if set to 1.",
            exclude=True,  # this prevents it from being merged
        )
        input_directory: Path = Field(
            default=Path("/data"), description="Input directory", exclude=True
        )
        model_config: ClassVar[SettingsConfigDict] = {
            "cli_parse_args": True,
        }

    CLIModel.__name__ = f"{model_cls.__name__}CLI"
    return CLIModel



def run_analysis_jobs(
    analysis_input_model: Type[GenericModel],
    analysis_output_model: Type[GenericModel],
    run_function: Callable[[GenericModel, GenericModel], dict[str, Any]],
) -> None:
    """
    Prepare and execute analysis jobs

    Parameters
    ----------
    analysis_input_model : GenericModel
        The analysis input model class used to validate
         parameters for each job.

    Returns
    -------
    """
    cli_cls = make_cli_model_class(analysis_input_model)
    cli_model = cli_cls()
    # parse CLI params for single-capsule app panel run
    cli_params = cli_model.model_dump(exclude_unset=True)
    logger.info(f"App panel CLI parameter overrides {cli_params}")
    input_model_paths = tuple(cli_model.input_directory.glob("job_dict/*"))
    logger.info(f"Found {len(input_model_paths)} input job models to run analysis on.")
    dry_run = bool(cli_model.dry_run)
    s3_bucket = os.getenv("ANALYSIS_BUCKET")

    for model_path in input_model_paths:
        with open(model_path, "r") as f:
            analysis_dispatch_inputs = AnalysisDispatchModel.model_validate_json(
                f.read()
            )
        logger.info(f"Running analysis for input model {model_path}")
        dispatch_params = analysis_dispatch_inputs.analysis_code.parameters.model_dump()
        logger.info(f"Dispatch parameters {dispatch_params}")
        analysis_specification = analysis_input_model.model_validate(
            dispatch_params | cli_params
        )
        analysis_dispatch_inputs.analysis_code.parameters = analysis_specification

        processing = construct_processing_record(analysis_dispatch_inputs)
        output_params = run_function(analysis_dispatch_inputs, analysis_specification)
        processing.output_parameters = analysis_output_model(**output_params)
        write_results_and_metadata(
            processing, s3_bucket=s3_bucket, dry_run=dry_run
        )
