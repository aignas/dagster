from typing import Optional, Sequence
from unittest.mock import PropertyMock, patch

import dagster._check as check
import pendulum
from dagster import AssetKey, RunRequest
from dagster._core.definitions.asset_condition import (
    AssetConditionEvaluation,
    AssetConditionEvaluationWithRunIds,
    AssetConditionSnapshot,
)
from dagster._core.definitions.asset_daemon_cursor import AssetDaemonCursor
from dagster._core.definitions.asset_subset import AssetSubset
from dagster._core.definitions.auto_materialize_rule_evaluation import (
    deserialize_auto_materialize_asset_evaluation_to_asset_condition_evaluation_with_run_ids,
)
from dagster._core.definitions.partition import PartitionsDefinition, StaticPartitionsDefinition
from dagster._core.definitions.run_request import (
    InstigatorType,
)
from dagster._core.definitions.sensor_definition import SensorType
from dagster._core.host_representation.origin import (
    ExternalInstigatorOrigin,
)
from dagster._core.scheduler.instigation import (
    InstigatorState,
    InstigatorStatus,
    SensorInstigatorData,
    TickData,
    TickStatus,
)
from dagster._core.workspace.context import WorkspaceRequestContext
from dagster._daemon.asset_daemon import (
    _PRE_SENSOR_AUTO_MATERIALIZE_CURSOR_KEY,
    _PRE_SENSOR_AUTO_MATERIALIZE_INSTIGATOR_NAME,
    _PRE_SENSOR_AUTO_MATERIALIZE_ORIGIN_ID,
    _PRE_SENSOR_AUTO_MATERIALIZE_SELECTOR_ID,
)
from dagster._serdes import deserialize_value
from dagster._serdes.serdes import serialize_value
from dagster_graphql.test.utils import execute_dagster_graphql, infer_repository

from dagster_graphql_tests.graphql.graphql_context_test_suite import (
    ExecutingGraphQLContextTestMatrix,
)

TICKS_QUERY = """
query AssetDameonTicksQuery($dayRange: Int, $dayOffset: Int, $statuses: [InstigationTickStatus!], $limit: Int, $cursor: String, $beforeTimestamp: Float, $afterTimestamp: Float) {
    autoMaterializeTicks(dayRange: $dayRange, dayOffset: $dayOffset, statuses: $statuses, limit: $limit, cursor: $cursor, beforeTimestamp: $beforeTimestamp, afterTimestamp: $afterTimestamp) {
        id
        timestamp
        endTimestamp
        status
        requestedAssetKeys {
            path
        }
        requestedMaterializationsForAssets {
            assetKey {
                path
            }
            partitionKeys
        }
        requestedAssetMaterializationCount
        autoMaterializeAssetEvaluationId
    }
}
"""


def _create_tick(instance, status, timestamp, evaluation_id, run_requests=None, end_timestamp=None):
    return instance.create_tick(
        TickData(
            instigator_origin_id=_PRE_SENSOR_AUTO_MATERIALIZE_ORIGIN_ID,
            instigator_name=_PRE_SENSOR_AUTO_MATERIALIZE_INSTIGATOR_NAME,
            instigator_type=InstigatorType.AUTO_MATERIALIZE,
            status=status,
            timestamp=timestamp,
            end_timestamp=end_timestamp,
            selector_id=_PRE_SENSOR_AUTO_MATERIALIZE_SELECTOR_ID,
            run_ids=[],
            auto_materialize_evaluation_id=evaluation_id,
            run_requests=run_requests,
        )
    )


class TestAutoMaterializeTicks(ExecutingGraphQLContextTestMatrix):
    def test_get_tick_range(self, graphql_context):
        result = execute_dagster_graphql(
            graphql_context,
            TICKS_QUERY,
            variables={"dayRange": None, "dayOffset": None},
        )
        assert len(result.data["autoMaterializeTicks"]) == 0

        now = pendulum.now("UTC")
        end_timestamp = now.timestamp() + 20

        success_1 = _create_tick(
            graphql_context.instance,
            TickStatus.SUCCESS,
            now.timestamp(),
            end_timestamp=end_timestamp,
            evaluation_id=3,
            run_requests=[
                RunRequest(asset_selection=[AssetKey("foo"), AssetKey("bar")], partition_key="abc"),
                RunRequest(asset_selection=[AssetKey("bar")], partition_key="def"),
                RunRequest(asset_selection=[AssetKey("baz")], partition_key=None),
            ],
        )

        success_2 = _create_tick(
            graphql_context.instance,
            TickStatus.SUCCESS,
            now.subtract(days=1, hours=1).timestamp(),
            evaluation_id=2,
        )

        _create_tick(
            graphql_context.instance,
            TickStatus.SKIPPED,
            now.subtract(days=2, hours=1).timestamp(),
            evaluation_id=1,
        )

        result = execute_dagster_graphql(
            graphql_context,
            TICKS_QUERY,
            variables={"dayRange": None, "dayOffset": None},
        )
        assert len(result.data["autoMaterializeTicks"]) == 3

        result = execute_dagster_graphql(
            graphql_context,
            TICKS_QUERY,
            variables={"dayRange": 1, "dayOffset": None},
        )
        assert len(result.data["autoMaterializeTicks"]) == 1
        tick = result.data["autoMaterializeTicks"][0]
        assert tick["endTimestamp"] == end_timestamp
        assert tick["autoMaterializeAssetEvaluationId"] == 3
        assert sorted(tick["requestedAssetKeys"], key=lambda x: x["path"][0]) == [
            {"path": ["bar"]},
            {"path": ["baz"]},
            {"path": ["foo"]},
        ]

        asset_materializations = tick["requestedMaterializationsForAssets"]
        by_asset_key = {
            AssetKey.from_coercible(mat["assetKey"]["path"]).to_user_string(): mat["partitionKeys"]
            for mat in asset_materializations
        }

        assert {key: sorted(val) for key, val in by_asset_key.items()} == {
            "foo": ["abc"],
            "bar": ["abc", "def"],
            "baz": [],
        }

        assert tick["requestedAssetMaterializationCount"] == 4

        result = execute_dagster_graphql(
            graphql_context,
            TICKS_QUERY,
            variables={
                "beforeTimestamp": success_2.timestamp + 1,
                "afterTimestamp": success_2.timestamp - 1,
            },
        )
        assert len(result.data["autoMaterializeTicks"]) == 1
        tick = result.data["autoMaterializeTicks"][0]
        assert (
            tick["autoMaterializeAssetEvaluationId"]
            == success_2.tick_data.auto_materialize_evaluation_id
        )

        result = execute_dagster_graphql(
            graphql_context,
            TICKS_QUERY,
            variables={"dayRange": None, "dayOffset": None, "statuses": ["SUCCESS"]},
        )
        assert len(result.data["autoMaterializeTicks"]) == 2

        result = execute_dagster_graphql(
            graphql_context,
            TICKS_QUERY,
            variables={"dayRange": None, "dayOffset": None, "statuses": ["SUCCESS"], "limit": 1},
        )
        ticks = result.data["autoMaterializeTicks"]
        assert len(ticks) == 1
        assert ticks[0]["timestamp"] == success_1.timestamp
        assert (
            ticks[0]["autoMaterializeAssetEvaluationId"]
            == success_1.tick_data.auto_materialize_evaluation_id
        )

        cursor = ticks[0]["id"]

        result = execute_dagster_graphql(
            graphql_context,
            TICKS_QUERY,
            variables={
                "dayRange": None,
                "dayOffset": None,
                "statuses": ["SUCCESS"],
                "limit": 1,
                "cursor": cursor,
            },
        )
        ticks = result.data["autoMaterializeTicks"]
        assert len(ticks) == 1
        assert ticks[0]["timestamp"] == success_2.timestamp


FRAGMENTS = """
fragment unpartitionedEvaluationFields on UnpartitionedAssetConditionEvaluation {
    description
    startTimestamp
    endTimestamp
    status
}

fragment partitionedEvaluationFields on PartitionedAssetConditionEvaluation {
    description
    startTimestamp
    endTimestamp
    numTrue
    numFalse
    numSkipped
    trueSubset {
        subsetValue {
            isPartitioned
            partitionKeys
        }
    }
    falseSubset {
        subsetValue {
            isPartitioned
            partitionKeys
        }
    }
}

fragment evaluationFields on AssetConditionEvaluation {
    ... on UnpartitionedAssetConditionEvaluation {
        ...unpartitionedEvaluationFields
        childEvaluations {
            ...unpartitionedEvaluationFields
            childEvaluations {
                ...unpartitionedEvaluationFields
                childEvaluations {
                    ...unpartitionedEvaluationFields
                    childEvaluations {
                        ...unpartitionedEvaluationFields
                    }
                }
            }
        }
    }
    ... on PartitionedAssetConditionEvaluation {
        ...partitionedEvaluationFields
        childEvaluations {
            ...partitionedEvaluationFields
            childEvaluations {
                ...partitionedEvaluationFields
                childEvaluations {
                    ...partitionedEvaluationFields
                    childEvaluations {
                        ...partitionedEvaluationFields
                    }
                }
            }
        }
    }
}
"""
QUERY = (
    FRAGMENTS
    + """
query GetEvaluationsQuery($assetKey: AssetKeyInput!, $limit: Int!, $cursor: String) {
    assetNodeOrError(assetKey: $assetKey) {
        ... on AssetNode {
            currentAutoMaterializeEvaluationId
            automationPolicySensor {
                name
            }
        }
    }
    assetConditionEvaluationRecordsOrError(assetKey: $assetKey, limit: $limit, cursor: $cursor) {
        ... on AssetConditionEvaluationRecords {
            records {
                id
                numRequested
                assetKey {
                    path
                }
                evaluation {
                    ...evaluationFields
                }
            }
        }
    }
}
"""
)

QUERY_FOR_SPECIFIC_PARTITION = """
fragment specificPartitionEvaluationFields on SpecificPartitionAssetConditionEvaluation {
    description
    status
}
query GetPartitionEvaluationQuery($assetKey: AssetKeyInput!, $partition: String!, $evaluationId: Int!) {
    assetConditionEvaluationForPartition(assetKey: $assetKey, partition: $partition, evaluationId: $evaluationId) {
        ...specificPartitionEvaluationFields
        childEvaluations {
            ...specificPartitionEvaluationFields
            childEvaluations {
                ...specificPartitionEvaluationFields
                childEvaluations {
                    ...specificPartitionEvaluationFields
                    childEvaluations {
                        ...specificPartitionEvaluationFields
                    }
                }
            }
        }
    }
}
"""

QUERY_FOR_EVALUATION_ID = (
    FRAGMENTS
    + """
query GetEvaluationsForEvaluationIdQuery($evaluationId: Int!) {
    assetConditionEvaluationsForEvaluationId(evaluationId: $evaluationId) {
        ... on AssetConditionEvaluationRecords {
            records {
                id
                numRequested
                assetKey {
                    path
                }
                evaluation {
                    ...evaluationFields
                }
            }
        }
    }
}
"""
)


class TestAutoMaterializeAssetEvaluations(ExecutingGraphQLContextTestMatrix):
    def test_automation_policy_sensor(self, graphql_context: WorkspaceRequestContext):
        sensor_origin = ExternalInstigatorOrigin(
            external_repository_origin=infer_repository(graphql_context).get_external_origin(),
            instigator_name="my_automation_policy_sensor",
        )

        check.not_none(graphql_context.instance.schedule_storage).add_instigator_state(
            InstigatorState(
                sensor_origin,
                InstigatorType.SENSOR,
                status=InstigatorStatus.RUNNING,
                instigator_data=SensorInstigatorData(
                    sensor_type=SensorType.AUTOMATION_POLICY,
                    cursor=serialize_value(AssetDaemonCursor.empty(12345)),
                ),
            )
        )

        with patch(
            "dagster._core.instance.DagsterInstance.auto_materialize_use_automation_policy_sensors",
            new_callable=PropertyMock,
        ) as mock_my_property:
            mock_my_property.return_value = True

            results = execute_dagster_graphql(
                graphql_context,
                QUERY,
                variables={
                    "assetKey": {"path": ["fresh_diamond_bottom"]},
                    "limit": 10,
                    "cursor": None,
                },
            )
            assert (
                results.data["assetNodeOrError"]["automationPolicySensor"]["name"]
                == "my_automation_policy_sensor"
            )
            assert results.data["assetNodeOrError"]["currentAutoMaterializeEvaluationId"] == 12345

    def test_get_historic_evaluation_without_evaluation_data(
        self, graphql_context: WorkspaceRequestContext
    ):
        """Ensures that we don't error out when faced with old format evaluations which do not
        contain much of the necessary data.
        """
        asset1_eval_str = '{"__class__": "AutoMaterializeAssetEvaluation", "asset_key": {"__class__": "AssetKey", "path": ["asset_one"]}, "num_discarded": 0, "num_requested": 0, "num_skipped": 0, "partition_subsets_by_condition": [], "rule_snapshots": null, "run_ids": {"__set__": []}}'
        asset2_eval_str = '{"__class__": "AutoMaterializeAssetEvaluation", "asset_key": {"__class__": "AssetKey", "path": ["asset_two"]}, "num_discarded": 0, "num_requested": 1, "num_skipped": 0, "partition_subsets_by_condition": [], "rule_snapshots": [{"__class__": "AutoMaterializeRuleSnapshot", "class_name": "MaterializeOnMissingRule", "decision_type": {"__enum__": "AutoMaterializeDecisionType.MATERIALIZE"}, "description": "materialization is missing"}], "run_ids": {"__set__": []}}'

        asset_evaluations = [
            deserialize_auto_materialize_asset_evaluation_to_asset_condition_evaluation_with_run_ids(
                eval_str, None
            )
            for eval_str in [asset1_eval_str, asset2_eval_str]
        ]
        check.not_none(
            graphql_context.instance.schedule_storage
        ).add_auto_materialize_asset_evaluations(
            evaluation_id=10, asset_evaluations=asset_evaluations
        )

        results = execute_dagster_graphql(
            graphql_context,
            QUERY,
            variables={"assetKey": {"path": ["asset_one"]}, "limit": 10, "cursor": None},
        )
        assert len(results.data["assetConditionEvaluationRecordsOrError"]["records"]) == 1
        asset_one_record = results.data["assetConditionEvaluationRecordsOrError"]["records"][0]
        assert asset_one_record["assetKey"] == {"path": ["asset_one"]}
        assert asset_one_record["evaluation"]["status"] == "SKIPPED"

        results = execute_dagster_graphql(
            graphql_context,
            QUERY,
            variables={"assetKey": {"path": ["asset_two"]}, "limit": 10, "cursor": None},
        )
        assert len(results.data["assetConditionEvaluationRecordsOrError"]["records"]) == 1
        asset_two_record = results.data["assetConditionEvaluationRecordsOrError"]["records"][0]
        assert asset_two_record["evaluation"]["description"] == "All of"
        assert asset_two_record["evaluation"]["status"] == "SKIPPED"
        asset_two_children = asset_two_record["evaluation"]["childEvaluations"]
        assert len(asset_two_children) == 2
        assert asset_two_children[0]["description"] == "Any of"
        assert asset_two_children[0]["status"] == "SKIPPED"
        assert (
            asset_two_children[0]["childEvaluations"][0]["description"]
            == "materialization is missing"
        )

        results = execute_dagster_graphql(
            graphql_context,
            QUERY_FOR_EVALUATION_ID,
            variables={"evaluationId": 10},
        )

        records = results.data["assetConditionEvaluationsForEvaluationId"]["records"]

        assert len(records) == 2

        # record from both previous queries are contained here
        assert any(record == asset_one_record for record in records)
        assert any(record == asset_two_record for record in records)

        results = execute_dagster_graphql(
            graphql_context,
            QUERY_FOR_EVALUATION_ID,
            variables={"evaluationId": 12345},
        )

        records = results.data["assetConditionEvaluationsForEvaluationId"]["records"]
        assert len(records) == 0

    def test_get_historic_evaluation_with_evaluation_data(
        self, graphql_context: WorkspaceRequestContext
    ):
        partitions_def = StaticPartitionsDefinition(["a", "b", "c", "d", "e", "f"])
        asset_eval_str = '{"__class__": "AutoMaterializeAssetEvaluation", "asset_key": {"__class__": "AssetKey", "path": ["upstream_static_partitioned_asset"]}, "num_discarded": 0, "num_requested": 0, "num_skipped": 1, "partition_subsets_by_condition": [[{"__class__": "AutoMaterializeRuleEvaluation", "evaluation_data": {"__class__": "WaitingOnAssetsRuleEvaluationData", "waiting_on_asset_keys": {"__frozenset__": [{"__class__": "AssetKey", "path": ["blah"]}]}}, "rule_snapshot": {"__class__": "AutoMaterializeRuleSnapshot", "class_name": "SkipOnRequiredButNonexistentParentsRule", "decision_type": {"__enum__": "AutoMaterializeDecisionType.SKIP"}, "description": "required parent partitions do not exist"}}, {"__class__": "SerializedPartitionsSubset", "serialized_partitions_def_class_name": "StaticPartitionsDefinition", "serialized_partitions_def_unique_id": "7c2047f8b02e90a69136c1a657bd99ad80b433a2", "serialized_subset": "{\\"version\\": 1, \\"subset\\": [\\"a\\"]}"}]], "rule_snapshots": null, "run_ids": {"__set__": []}}'
        asset_evaluations = [
            deserialize_auto_materialize_asset_evaluation_to_asset_condition_evaluation_with_run_ids(
                asset_eval_str, partitions_def
            )
        ]

        check.not_none(
            graphql_context.instance.schedule_storage
        ).add_auto_materialize_asset_evaluations(
            evaluation_id=10, asset_evaluations=asset_evaluations
        )

        results = execute_dagster_graphql(
            graphql_context,
            QUERY,
            variables={
                "assetKey": {"path": ["upstream_static_partitioned_asset"]},
                "limit": 10,
                "cursor": None,
            },
        )

        records = results.data["assetConditionEvaluationRecordsOrError"]["records"]
        assert len(records) == 1
        evaluation = records[0]["evaluation"]
        assert evaluation["numTrue"] == 0
        assert evaluation["numFalse"] == 6
        assert evaluation["numSkipped"] == 0
        assert len(evaluation["childEvaluations"]) == 2
        not_skip_evaluation = evaluation["childEvaluations"][1]
        assert not_skip_evaluation["description"] == "Not"
        assert not_skip_evaluation["numTrue"] == 1
        assert len(not_skip_evaluation["childEvaluations"]) == 1
        assert not_skip_evaluation["childEvaluations"][0]["description"] == "Any of"
        assert len(not_skip_evaluation["childEvaluations"][0]["childEvaluations"]) == 2

    def _test_get_evaluations(self, graphql_context: WorkspaceRequestContext):
        results = execute_dagster_graphql(
            graphql_context,
            QUERY,
            variables={"assetKey": {"path": ["foo"]}, "limit": 10, "cursor": None},
        )
        assert results.data == {
            "autoMaterializeAssetEvaluationsOrError": {"records": []},
        }

        check.not_none(
            graphql_context.instance.schedule_storage
        ).add_auto_materialize_asset_evaluations(
            evaluation_id=10,
            asset_evaluations=deserialize_value(
                '[{"__class__": "AutoMaterializeAssetEvaluation", "asset_key": {"__class__": "AssetKey", "path": ["asset_one"]}, "num_discarded": 0, "num_requested": 0, "num_skipped": 0, "partition_subsets_by_condition": [], "rule_snapshots": null, "run_ids": {"__set__": []}}, {"__class__": "AutoMaterializeAssetEvaluation", "asset_key": {"__class__": "AssetKey", "path": ["asset_two"]}, "num_discarded": 0, "num_requested": 1, "num_skipped": 0, "partition_subsets_by_condition": [[{"__class__": "AutoMaterializeRuleEvaluation", "evaluation_data": null, "rule_snapshot": {"__class__": "AutoMaterializeRuleSnapshot", "class_name": "MaterializeOnMissingRule", "decision_type": {"__enum__": "AutoMaterializeDecisionType.MATERIALIZE"}, "description": "materialization is missing"}}, null]], "rule_snapshots": null, "run_ids": {"__set__": []}}, {"__class__": "AutoMaterializeAssetEvaluation", "asset_key": {"__class__": "AssetKey", "path": ["asset_three"]}, "num_discarded": 0, "num_requested": 0, "num_skipped": 1, "partition_subsets_by_condition": [[{"__class__": "AutoMaterializeRuleEvaluation", "evaluation_data": {"__class__": "WaitingOnAssetsRuleEvaluationData", "waiting_on_asset_keys": {"__frozenset__": [{"__class__": "AssetKey", "path": ["asset_two"]}]}}, "rule_snapshot": {"__class__": "AutoMaterializeRuleSnapshot", "class_name": "SkipOnParentOutdatedRule", "decision_type": {"__enum__": "AutoMaterializeDecisionType.SKIP"}, "description": "waiting on upstream data to be up to date"}}, null]], "rule_snapshots": null, "run_ids": {"__set__": []}}, {"__class__": "AutoMaterializeAssetEvaluation", "asset_key": {"__class__": "AssetKey", "path": ["asset_four"]}, "num_discarded": 0, "num_requested": 1, "num_skipped": 0, "partition_subsets_by_condition": [[{"__class__": "AutoMaterializeRuleEvaluation", "evaluation_data": {"__class__": "ParentUpdatedRuleEvaluationData", "updated_asset_keys": {"__frozenset__": [{"__class__": "AssetKey", "path": ["asset_two"]}]}, "will_update_asset_keys": {"__frozenset__": [{"__class__": "AssetKey", "path": ["asset_three"]}]}}, "rule_snapshot": {"__class__": "AutoMaterializeRuleSnapshot", "class_name": "MaterializeOnParentUpdatedRule", "decision_type": {"__enum__": "AutoMaterializeDecisionType.MATERIALIZE"}, "description": "upstream data has changed since latest materialization"}}, null]], "rule_snapshots": null, "run_ids": {"__set__": []}}]',
                Sequence,
            ),
        )

        results = execute_dagster_graphql(
            graphql_context,
            QUERY,
            variables={"assetKey": {"path": ["asset_one"]}, "limit": 10, "cursor": None},
        )
        assert results.data == {
            "assetNodeOrError": {
                "currentAutoMaterializeEvaluationId": None,
                "automationPolicySensor": None,
            },
            "autoMaterializeAssetEvaluationsOrError": {
                "records": [
                    {
                        "numRequested": 0,
                        "numSkipped": 0,
                        "numDiscarded": 0,
                        "rulesWithRuleEvaluations": [],
                    }
                ],
            },
        }

        results = execute_dagster_graphql(
            graphql_context,
            QUERY,
            variables={"assetKey": {"path": ["asset_two"]}, "limit": 10, "cursor": None},
        )
        assert results.data == {
            "assetNodeOrError": {
                "currentAutoMaterializeEvaluationId": None,
                "automationPolicySensor": None,
            },
            "autoMaterializeAssetEvaluationsOrError": {
                "records": [
                    {
                        "numRequested": 1,
                        "numSkipped": 0,
                        "numDiscarded": 0,
                        "rulesWithRuleEvaluations": [
                            {
                                "rule": {"decisionType": "MATERIALIZE"},
                                "ruleEvaluations": [
                                    {
                                        "evaluationData": None,
                                        "partitionKeysOrError": None,
                                    }
                                ],
                            }
                        ],
                    }
                ],
            },
        }

        results = execute_dagster_graphql(
            graphql_context,
            QUERY,
            variables={"assetKey": {"path": ["asset_three"]}, "limit": 10, "cursor": None},
        )
        assert results.data == {
            "assetNodeOrError": {
                "currentAutoMaterializeEvaluationId": None,
                "automationPolicySensor": None,
            },
            "autoMaterializeAssetEvaluationsOrError": {
                "records": [
                    {
                        "numRequested": 0,
                        "numSkipped": 1,
                        "numDiscarded": 0,
                        "rulesWithRuleEvaluations": [
                            {
                                "rule": {"decisionType": "SKIP"},
                                "ruleEvaluations": [
                                    {
                                        "evaluationData": {
                                            "waitingOnAssetKeys": [{"path": ["asset_two"]}],
                                        },
                                        "partitionKeysOrError": None,
                                    }
                                ],
                            }
                        ],
                    }
                ],
            },
        }

        results = execute_dagster_graphql(
            graphql_context,
            QUERY,
            variables={"assetKey": {"path": ["asset_four"]}, "limit": 10, "cursor": None},
        )
        assert results.data == {
            "assetNodeOrError": {
                "currentAutoMaterializeEvaluationId": None,
                "automationPolicySensor": None,
            },
            "autoMaterializeAssetEvaluationsOrError": {
                "records": [
                    {
                        "numRequested": 1,
                        "numSkipped": 0,
                        "numDiscarded": 0,
                        "rulesWithRuleEvaluations": [
                            {
                                "rule": {"decisionType": "MATERIALIZE"},
                                "ruleEvaluations": [
                                    {
                                        "evaluationData": {
                                            "updatedAssetKeys": [{"path": ["asset_two"]}],
                                            "willUpdateAssetKeys": [{"path": ["asset_three"]}],
                                        },
                                        "partitionKeysOrError": None,
                                    }
                                ],
                            }
                        ],
                    }
                ],
            },
        }

    def _get_condition_evaluation(
        self,
        asset_key: AssetKey,
        description: str,
        partitions_def: PartitionsDefinition,
        true_partition_keys: Sequence[str],
        candidate_partition_keys: Optional[Sequence[str]] = None,
        child_evaluations: Optional[Sequence[AssetConditionEvaluation]] = None,
    ) -> AssetConditionEvaluation:
        return AssetConditionEvaluation(
            condition_snapshot=AssetConditionSnapshot("...", description, "a1b2"),
            true_subset=AssetSubset(
                asset_key=asset_key,
                value=partitions_def.subset_with_partition_keys(true_partition_keys),
            ),
            candidate_subset=AssetSubset(
                asset_key=asset_key,
                value=partitions_def.subset_with_partition_keys(candidate_partition_keys),
            )
            if candidate_partition_keys
            else None,
            start_timestamp=123,
            end_timestamp=456,
            child_evaluations=child_evaluations or [],
        )

    def test_get_evaluations_with_partitions(self, graphql_context: WorkspaceRequestContext):
        asset_key = AssetKey("upstream_static_partitioned_asset")
        partitions_def = StaticPartitionsDefinition(["a", "b", "c", "d", "e", "f"])
        results = execute_dagster_graphql(
            graphql_context,
            QUERY,
            variables={
                "assetKey": {"path": ["upstream_static_partitioned_asset"]},
                "limit": 10,
                "cursor": None,
            },
        )
        assert results.data == {
            "assetNodeOrError": {
                "currentAutoMaterializeEvaluationId": None,
                "automationPolicySensor": None,
            },
            "assetConditionEvaluationRecordsOrError": {"records": []},
        }

        evaluation = self._get_condition_evaluation(
            asset_key,
            "All of",
            partitions_def,
            ["a", "b"],
            child_evaluations=[
                self._get_condition_evaluation(
                    asset_key,
                    "Any of",
                    partitions_def,
                    ["a", "b", "c"],
                    child_evaluations=[
                        self._get_condition_evaluation(
                            asset_key, "parent_updated", partitions_def, ["a", "c"]
                        ),
                        self._get_condition_evaluation(asset_key, "missing", partitions_def, ["b"]),
                        self._get_condition_evaluation(asset_key, "other", partitions_def, []),
                    ],
                ),
                self._get_condition_evaluation(
                    asset_key,
                    "Not",
                    partitions_def,
                    ["a", "b"],
                    candidate_partition_keys=["a", "b", "c"],
                    child_evaluations=[
                        self._get_condition_evaluation(
                            asset_key,
                            "Any of",
                            partitions_def,
                            ["c"],
                            ["a", "b", "c"],
                            child_evaluations=[
                                self._get_condition_evaluation(
                                    asset_key,
                                    "parent missing",
                                    partitions_def,
                                    ["c"],
                                    ["a", "b", "c"],
                                ),
                                self._get_condition_evaluation(
                                    asset_key,
                                    "parent outdated",
                                    partitions_def,
                                    [],
                                    ["a", "b", "c"],
                                ),
                            ],
                        ),
                    ],
                ),
            ],
        )

        check.not_none(
            graphql_context.instance.schedule_storage
        ).add_auto_materialize_asset_evaluations(
            evaluation_id=10,
            asset_evaluations=[
                AssetConditionEvaluationWithRunIds(evaluation, frozenset({"runid1", "runid2"}))
            ],
        )

        results = execute_dagster_graphql(
            graphql_context,
            QUERY,
            variables={
                "assetKey": {"path": ["upstream_static_partitioned_asset"]},
                "limit": 10,
                "cursor": None,
            },
        )

        records = results.data["assetConditionEvaluationRecordsOrError"]["records"]
        assert len(records) == 1

        assert records[0]["numRequested"] == 2
        evaluation = records[0]["evaluation"]
        assert evaluation["description"] == "All of"
        assert evaluation["numTrue"] == 2
        assert evaluation["numFalse"] == 4
        assert evaluation["numSkipped"] == 0
        assert set(evaluation["trueSubset"]["subsetValue"]["partitionKeys"]) == {"a", "b"}
        assert len(evaluation["childEvaluations"]) == 2

        not_evaluation = evaluation["childEvaluations"][1]
        assert not_evaluation["description"] == "Not"
        assert not_evaluation["numTrue"] == 2
        assert not_evaluation["numFalse"] == 1
        assert not_evaluation["numSkipped"] == 3
        assert set(not_evaluation["trueSubset"]["subsetValue"]["partitionKeys"]) == {"a", "b"}

        skip_evaluation = not_evaluation["childEvaluations"][0]
        assert skip_evaluation["description"] == "Any of"
        assert skip_evaluation["numTrue"] == 1
        assert skip_evaluation["numFalse"] == 2
        assert skip_evaluation["numSkipped"] == 3
        assert set(skip_evaluation["trueSubset"]["subsetValue"]["partitionKeys"]) == {"c"}

        # test one of the true partitions
        specific_result = execute_dagster_graphql(
            graphql_context,
            QUERY_FOR_SPECIFIC_PARTITION,
            variables={
                "assetKey": {"path": ["upstream_static_partitioned_asset"]},
                "partition": "b",
                "evaluationId": 10,
            },
        )

        evaluation = specific_result.data["assetConditionEvaluationForPartition"]
        assert evaluation["description"] == "All of"
        assert evaluation["status"] == "TRUE"
        assert len(evaluation["childEvaluations"]) == 2

        not_evaluation = evaluation["childEvaluations"][1]
        assert not_evaluation["description"] == "Not"
        assert not_evaluation["status"] == "TRUE"

        skip_evaluation = not_evaluation["childEvaluations"][0]
        assert skip_evaluation["description"] == "Any of"
        assert skip_evaluation["status"] == "FALSE"

        # test one of the false partitions
        specific_result = execute_dagster_graphql(
            graphql_context,
            QUERY_FOR_SPECIFIC_PARTITION,
            variables={
                "assetKey": {"path": ["upstream_static_partitioned_asset"]},
                "partition": "d",
                "evaluationId": 10,
            },
        )

        evaluation = specific_result.data["assetConditionEvaluationForPartition"]
        assert evaluation["description"] == "All of"
        assert evaluation["status"] == "FALSE"
        assert len(evaluation["childEvaluations"]) == 2

        not_evaluation = evaluation["childEvaluations"][1]
        assert not_evaluation["description"] == "Not"
        assert not_evaluation["status"] == "SKIPPED"

        skip_evaluation = not_evaluation["childEvaluations"][0]
        assert skip_evaluation["description"] == "Any of"
        assert skip_evaluation["status"] == "SKIPPED"

    def _test_current_evaluation_id(self, graphql_context: WorkspaceRequestContext):
        graphql_context.instance.daemon_cursor_storage.set_cursor_values(
            {_PRE_SENSOR_AUTO_MATERIALIZE_CURSOR_KEY: serialize_value(AssetDaemonCursor.empty(0))}
        )

        results = execute_dagster_graphql(
            graphql_context,
            QUERY,
            variables={"assetKey": {"path": ["asset_two"]}, "limit": 10, "cursor": None},
        )
        assert results.data == {
            "assetNodeOrError": {
                "currentAutoMaterializeEvaluationId": 0,
                "automationPolicySensor": None,
            },
            "autoMaterializeAssetEvaluationsOrError": {
                "records": [],
            },
        }

        graphql_context.instance.daemon_cursor_storage.set_cursor_values(
            {
                _PRE_SENSOR_AUTO_MATERIALIZE_CURSOR_KEY: (
                    serialize_value(AssetDaemonCursor.empty(0).with_updates(0, 1.0, [], []))
                )
            }
        )

        results = execute_dagster_graphql(
            graphql_context,
            QUERY,
            variables={"assetKey": {"path": ["asset_two"]}, "limit": 10, "cursor": None},
        )
        assert results.data == {
            "assetNodeOrError": {
                "currentAutoMaterializeEvaluationId": 42,
                "automationPolicySensor": None,
            },
            "autoMaterializeAssetEvaluationsOrError": {
                "records": [],
            },
        }
