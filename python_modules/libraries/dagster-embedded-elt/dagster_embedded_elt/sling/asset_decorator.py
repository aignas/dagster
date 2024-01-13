import re
from typing import Any, Callable, Iterable, Mapping, Optional

from dagster import (
    AssetKey,
    AssetOut,
    AssetsDefinition,
    BackfillPolicy,
    PartitionsDefinition,
    multi_asset,
)
from dagster._core.definitions.asset_dep import CoercibleToAssetDep

from dagster_embedded_elt.sling.sling_replication import SlingReplicationParam, validate_replication


def get_streams_from_replication(replication: Mapping[str, Any]) -> Iterable[str]:
    """Returns the set of streams from a Sling replication config."""
    keys = []

    for key, value in replication["streams"].items():
        if isinstance(value, dict):
            asset_key_config = value.get("meta", {}).get("dagster", {}).get("asset_key", [])
            keys.append(asset_key_config if asset_key_config else key)
        else:
            keys.append(key)
    return keys


def sling_stream_to_output_name(stream: str) -> str:
    stream = stream.replace('"', "")
    return re.sub(r"[^a-zA-Z0-9_.]", "_", stream).split(".")[-1]


def sling_stream_to_asset_key(stream: str) -> AssetKey:
    stream = stream.replace('"', "")
    stream = re.sub(r"[^a-zA-Z0-9_.]", "_", stream)
    components = stream.split(".")
    return AssetKey(components)


def sling_assets(
    *,
    replication: SlingReplicationParam,
    deps: Optional[Iterable[CoercibleToAssetDep]] = None,
    name: Optional[str] = None,
    partitions_def: Optional[PartitionsDefinition] = None,
    backfill_policy: Optional[BackfillPolicy] = None,
    op_tags: Optional[Mapping[str, Any]] = None,
) -> Callable[..., AssetsDefinition]:
    replication = validate_replication(replication)
    streams = get_streams_from_replication(replication)

    outs = {}
    for stream in streams:
        key = sling_stream_to_asset_key(stream)
        outname = sling_stream_to_output_name(stream)
        outs[outname] = AssetOut(key=key)

    def inner(fn) -> AssetsDefinition:
        asset_definition = multi_asset(
            deps=deps,
            outs=outs,
            name=name,
            compute_kind="sling",
            partitions_def=partitions_def,
            can_subset=False,
            op_tags=op_tags,
            backfill_policy=backfill_policy,
        )(fn)

        return asset_definition

    return inner
