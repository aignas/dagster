from typing import TYPE_CHECKING, Union

import dagster._check as check
from dagster._core.instance import DagsterInstance
from dagster._core.storage.pipeline_run import DagsterRun

from dagster_graphql.schema.util import ResolveInfo

from .external import get_external_pipeline_or_raise, get_full_external_pipeline_or_raise
from .utils import PipelineSelector, UserFacingGraphQLError, capture_error

if TYPE_CHECKING:
    from ..schema.pipelines.pipeline import GraphenePipeline
    from ..schema.pipelines.pipeline_ref import GrapheneUnknownPipeline
    from ..schema.pipelines.snapshot import GraphenePipelineSnapshot


@capture_error
def get_pipeline_snapshot_or_error_from_pipeline_selector(
    graphene_info: ResolveInfo, pipeline_selector: PipelineSelector
) -> "GraphenePipelineSnapshot":
    from ..schema.pipelines.snapshot import GraphenePipelineSnapshot

    check.inst_param(pipeline_selector, "pipeline_selector", PipelineSelector)
    return GraphenePipelineSnapshot(
        get_full_external_pipeline_or_raise(graphene_info, pipeline_selector)
    )


@capture_error
def get_pipeline_snapshot_or_error_from_snapshot_id(
    graphene_info: ResolveInfo, snapshot_id: str
) -> "GraphenePipelineSnapshot":
    check.str_param(snapshot_id, "snapshot_id")
    return _get_pipeline_snapshot_from_instance(graphene_info.context.instance, snapshot_id)


# extracted this out to test
def _get_pipeline_snapshot_from_instance(
    instance: DagsterInstance, snapshot_id: str
) -> "GraphenePipelineSnapshot":
    from ..schema.errors import GraphenePipelineSnapshotNotFoundError
    from ..schema.pipelines.snapshot import GraphenePipelineSnapshot

    if not instance.has_job_snapshot(snapshot_id):
        raise UserFacingGraphQLError(GraphenePipelineSnapshotNotFoundError(snapshot_id))

    historical_pipeline = instance.get_historical_job(snapshot_id)

    if not historical_pipeline:
        # Either a temporary error or it has been deleted in the interim
        raise UserFacingGraphQLError(GraphenePipelineSnapshotNotFoundError(snapshot_id))

    return GraphenePipelineSnapshot(historical_pipeline)


@capture_error
def get_pipeline_or_error(
    graphene_info: ResolveInfo, selector: PipelineSelector
) -> "GraphenePipeline":
    """Returns a PipelineOrError."""
    return get_pipeline_from_selector(graphene_info, selector)


def get_pipeline_or_raise(
    graphene_info: ResolveInfo, selector: PipelineSelector
) -> "GraphenePipeline":
    """Returns a Pipeline or raises a UserFacingGraphQLError if one cannot be retrieved
    from the selector, e.g., the pipeline is not present in the loaded repository.
    """
    return get_pipeline_from_selector(graphene_info, selector)


def get_pipeline_reference_or_raise(
    graphene_info: ResolveInfo, pipeline_run: DagsterRun
) -> Union["GraphenePipelineSnapshot", "GrapheneUnknownPipeline"]:
    """Returns a PipelineReference or raises a UserFacingGraphQLError if a pipeline
    reference cannot be retrieved based on the run, e.g, a UserFacingGraphQLError that wraps an
    InvalidSubsetError.
    """
    from ..schema.pipelines.pipeline_ref import GrapheneUnknownPipeline

    check.inst_param(pipeline_run, "pipeline_run", DagsterRun)
    solid_selection = (
        list(pipeline_run.solids_to_execute) if pipeline_run.solids_to_execute else None
    )

    if pipeline_run.job_snapshot_id is None:
        return GrapheneUnknownPipeline(pipeline_run.job_name, solid_selection)

    return _get_pipeline_snapshot_from_instance(
        graphene_info.context.instance, pipeline_run.job_snapshot_id
    )


def get_pipeline_from_selector(
    graphene_info: ResolveInfo, selector: PipelineSelector
) -> "GraphenePipeline":
    from ..schema.pipelines.pipeline import GraphenePipeline

    check.inst_param(selector, "selector", PipelineSelector)

    return GraphenePipeline(get_external_pipeline_or_raise(graphene_info, selector))
