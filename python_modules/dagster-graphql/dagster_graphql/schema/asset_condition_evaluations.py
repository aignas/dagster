import enum
from typing import Optional, Sequence, Union

import graphene
import pendulum
from dagster._core.definitions.asset_condition import AssetConditionEvaluation
from dagster._core.definitions.asset_subset import AssetSubset
from dagster._core.definitions.partition import PartitionsDefinition, PartitionsSubset
from dagster._core.definitions.time_window_partitions import BaseTimeWindowPartitionsSubset
from dagster._core.instance import DynamicPartitionsStore
from dagster._core.scheduler.instigation import AutoMaterializeAssetEvaluationRecord

from dagster_graphql.schema.auto_materialize_asset_evaluations import (
    GrapheneAutoMaterializeAssetEvaluationNeedsMigrationError,
)
from dagster_graphql.schema.metadata import GrapheneMetadataEntry

from .asset_key import GrapheneAssetKey
from .partition_sets import GraphenePartitionKeyRange
from .util import ResolveInfo, non_null_list


class AssetConditionEvaluationStatus(enum.Enum):
    TRUE = "TRUE"
    FALSE = "FALSE"
    SKIPPED = "SKIPPED"


GrapheneAssetConditionEvaluationStatus = graphene.Enum.from_enum(AssetConditionEvaluationStatus)


class GrapheneAssetSubsetValue(graphene.ObjectType):
    class Meta:
        name = "AssetSubsetValue"

    boolValue = graphene.Field(graphene.Boolean)
    partitionKeys = graphene.List(graphene.NonNull(graphene.String))
    partitionKeyRanges = graphene.List(graphene.NonNull(GraphenePartitionKeyRange))

    isPartitioned = graphene.NonNull(graphene.Boolean)

    def __init__(self, value: Union[bool, PartitionsSubset]):
        bool_value, partition_keys, partition_key_ranges = None, None, None
        if isinstance(value, bool):
            bool_value = value
        elif isinstance(value, BaseTimeWindowPartitionsSubset):
            partition_key_ranges = [
                GraphenePartitionKeyRange(start, end)
                for start, end in value.get_partition_key_ranges(value.partitions_def)
            ]
        else:
            partition_keys = value.get_partition_keys()

        super().__init__(
            boolValue=bool_value,
            partitionKeys=partition_keys,
            partitionKeyRanges=partition_key_ranges,
        )

    def resolve_isPartitioned(self, graphene_info: ResolveInfo) -> bool:
        return self.boolValue is not None


class GrapheneAssetSubset(graphene.ObjectType):
    assetKey = graphene.NonNull(GrapheneAssetKey)
    subsetValue = graphene.NonNull(GrapheneAssetSubsetValue)

    class Meta:
        name = "AssetSubset"

    def __init__(self, asset_subset: AssetSubset):
        super().__init__(
            assetKey=GrapheneAssetKey(path=asset_subset.asset_key.path),
            subsetValue=GrapheneAssetSubsetValue(asset_subset.subset_value),
        )


class GrapheneUnpartitionedAssetConditionEvaluation(graphene.ObjectType):
    description = graphene.NonNull(graphene.String)

    startTimestamp = graphene.Field(graphene.Float)
    endTimestamp = graphene.Field(graphene.Float)

    metadataEntries = non_null_list(GrapheneMetadataEntry)
    status = graphene.NonNull(GrapheneAssetConditionEvaluationStatus)

    childEvaluations = graphene.Field(
        graphene.List(graphene.NonNull(lambda: GrapheneUnpartitionedAssetConditionEvaluation))
    )

    class Meta:
        name = "UnpartitionedAssetConditionEvaluation"

    def __init__(self, evaluation: AssetConditionEvaluation):
        if evaluation.true_subset.bool_value:
            status = AssetConditionEvaluationStatus.TRUE
        elif evaluation.candidate_subset and evaluation.candidate_subset.bool_value:
            status = AssetConditionEvaluationStatus.FALSE
        else:
            status = AssetConditionEvaluationStatus.SKIPPED

        super().__init__(
            description=evaluation.condition_snapshot.description,
            startTimestamp=evaluation.start_timestamp,
            endTimestamp=evaluation.end_timestamp,
            status=status,
            childEvaluations=[
                GrapheneUnpartitionedAssetConditionEvaluation(child)
                for child in evaluation.child_evaluations
            ],
        )

    def resolve_metadataEntries(
        self, graphene_info: ResolveInfo
    ) -> Sequence[GrapheneMetadataEntry]:
        metadata = next(
            (subset.metadata for subset in self._evaluation.subsets_with_metadata),
            {},
        )
        return [GrapheneMetadataEntry(key=key, value=value) for key, value in metadata.items()]


class GraphenePartitionedAssetConditionEvaluation(graphene.ObjectType):
    description = graphene.NonNull(graphene.String)

    startTimestamp = graphene.Field(graphene.Float)
    endTimestamp = graphene.Field(graphene.Float)

    trueSubset = graphene.NonNull(GrapheneAssetSubset)
    falseSubset = graphene.NonNull(GrapheneAssetSubset)
    candidateSubset = graphene.Field(GrapheneAssetSubset)

    numTrue = graphene.NonNull(graphene.Int)
    numFalse = graphene.NonNull(graphene.Int)
    numSkipped = graphene.NonNull(graphene.Int)

    childEvaluations = graphene.Field(
        graphene.List(graphene.NonNull(lambda: GraphenePartitionedAssetConditionEvaluation))
    )

    class Meta:
        name = "PartitionedAssetConditionEvaluation"

    def __init__(
        self,
        evaluation: AssetConditionEvaluation,
        partitions_def: Optional[PartitionsDefinition],
        dynamic_partitions_store: DynamicPartitionsStore,
    ):
        self._partitions_def = partitions_def
        self._true_subset = evaluation.true_subset

        self._all_subset = AssetSubset.all(
            evaluation.asset_key, partitions_def, dynamic_partitions_store, pendulum.now("UTC")
        )

        # if the candidate_subset is unset, then we evaluated all partitions
        self._candidate_subset = evaluation.candidate_subset or self._all_subset

        super().__init__(
            description=evaluation.condition_snapshot.description,
            startTimestamp=evaluation.start_timestamp,
            endTimestamp=evaluation.end_timestamp,
            trueSubset=GrapheneAssetSubset(evaluation.true_subset),
            candidateSubset=GrapheneAssetSubset(self._candidate_subset),
            childEvaluations=[
                GraphenePartitionedAssetConditionEvaluation(
                    child, partitions_def, dynamic_partitions_store
                )
                for child in evaluation.child_evaluations
            ],
        )

    def resolve_numTrue(self, graphene_info: ResolveInfo) -> int:
        return self._true_subset.size

    def resolve_numFalse(self, graphene_info: ResolveInfo) -> int:
        return self._candidate_subset.size - self._true_subset.size

    def resolve_falseSubset(self, graphene_info: ResolveInfo) -> GrapheneAssetSubset:
        return GrapheneAssetSubset(self._candidate_subset - self._true_subset)

    def resolve_numSkipped(self, graphene_info: ResolveInfo) -> int:
        return self._all_subset.size - self._candidate_subset.size


class GrapheneSpecificPartitionAssetConditionEvaluation(graphene.ObjectType):
    description = graphene.NonNull(graphene.String)

    metadataEntries = non_null_list(GrapheneMetadataEntry)
    status = graphene.NonNull(GrapheneAssetConditionEvaluationStatus)

    childEvaluations = graphene.Field(
        graphene.List(graphene.NonNull(lambda: GrapheneSpecificPartitionAssetConditionEvaluation))
    )

    class Meta:
        name = "SpecificPartitionAssetConditionEvaluation"

    def __init__(self, evaluation: AssetConditionEvaluation, partition_key: str):
        self._evaluation = evaluation
        self._partition_key = partition_key

        if partition_key in evaluation.true_subset.subset_value:
            status = AssetConditionEvaluationStatus.TRUE
        elif (
            evaluation.candidate_subset is None
            or partition_key in evaluation.candidate_subset.subset_value
        ):
            status = AssetConditionEvaluationStatus.FALSE
        else:
            status = AssetConditionEvaluationStatus.SKIPPED

        super().__init__(
            description=evaluation.condition_snapshot.description,
            status=status,
            childEvaluations=[
                GrapheneSpecificPartitionAssetConditionEvaluation(child, partition_key)
                for child in evaluation.child_evaluations
            ],
        )

    def resolve_metadataEntries(
        self, graphene_info: ResolveInfo
    ) -> Sequence[GrapheneMetadataEntry]:
        # find the metadata associated with a subset that contains this partition key
        metadata = next(
            (
                subset.metadata
                for subset in self._evaluation.subsets_with_metadata
                if self._partition_key in subset.subset.subset_value
            ),
            {},
        )
        return [GrapheneMetadataEntry(key=key, value=value) for key, value in metadata.items()]


class GrapheneAssetConditionEvaluation(graphene.Union):
    class Meta:
        types = (
            GrapheneUnpartitionedAssetConditionEvaluation,
            GraphenePartitionedAssetConditionEvaluation,
            GrapheneSpecificPartitionAssetConditionEvaluation,
        )
        name = "AssetConditionEvaluation"


class GrapheneAssetConditionEvaluationRecord(graphene.ObjectType):
    id = graphene.NonNull(graphene.ID)
    evaluationId = graphene.NonNull(graphene.Int)
    runIds = non_null_list(graphene.String)
    timestamp = graphene.NonNull(graphene.Float)

    assetKey = graphene.NonNull(GrapheneAssetKey)
    numRequested = graphene.NonNull(graphene.Int)

    evaluation = graphene.NonNull(GrapheneAssetConditionEvaluation)

    class Meta:
        name = "AssetConditionEvaluationRecord"

    def __init__(
        self,
        record: AutoMaterializeAssetEvaluationRecord,
        partitions_def: Optional[PartitionsDefinition],
        dynamic_partitions_store: DynamicPartitionsStore,
        partition_key: Optional[str] = None,
    ):
        evaluation_with_run_ids = record.get_evaluation_with_run_ids(partitions_def)
        if evaluation_with_run_ids.evaluation.true_subset.is_partitioned:
            if partition_key is None:
                evaluation = GraphenePartitionedAssetConditionEvaluation(
                    evaluation_with_run_ids.evaluation, partitions_def, dynamic_partitions_store
                )
            else:
                evaluation = GrapheneSpecificPartitionAssetConditionEvaluation(
                    evaluation_with_run_ids.evaluation, partition_key
                )
        else:
            evaluation = GrapheneUnpartitionedAssetConditionEvaluation(
                evaluation_with_run_ids.evaluation
            )

        super().__init__(
            id=record.id,
            evaluationId=record.evaluation_id,
            timestamp=record.timestamp,
            runIds=evaluation_with_run_ids.run_ids,
            assetKey=GrapheneAssetKey(path=record.asset_key.path),
            numRequested=evaluation_with_run_ids.evaluation.true_subset.size,
            evaluation=evaluation,
        )


class GrapheneAssetConditionEvaluationRecords(graphene.ObjectType):
    records = non_null_list(GrapheneAssetConditionEvaluationRecord)

    class Meta:
        name = "AssetConditionEvaluationRecords"


class GrapheneAssetConditionEvaluationRecordsOrError(graphene.Union):
    class Meta:
        types = (
            GrapheneAssetConditionEvaluationRecords,
            GrapheneAutoMaterializeAssetEvaluationNeedsMigrationError,
        )
        name = "AssetConditionEvaluationRecordsOrError"
