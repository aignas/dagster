import os
from pathlib import Path

import pytest
import yaml
from dagster import AssetKey
from dagster_embedded_elt.sling.asset_decorator import sling_assets
from dagster_embedded_elt.sling.sling_replication import SlingReplicationParam

"""@pytest.mark.parametrize("manifest", [manifest, manifest_path, os.fspath(manifest_path)])
def test_manifest_argument(manifest: DbtManifestParam):
    @dbt_assets(manifest=manifest)
    def my_dbt_assets():
        ...

    assert my_dbt_assets.keys == {
        AssetKey.from_user_string(key)
        for key in [
            "sort_by_calories",
            "cold_schema/sort_cold_cereals_by_calories",
            "subdir_schema/least_caloric",
            "sort_hot_cereals_by_calories",
            "orders_snapshot",
            "cereals",
        ]
    }
"""


replication_path = Path(__file__).joinpath("..", "sling_replication.yaml").resolve()
with replication_path.open("r") as f:
    replication = yaml.safe_load(f)


@pytest.mark.parametrize(
    "replication", [replication, replication_path, os.fspath(replication_path)]
)
def test_replication_argument(replication: SlingReplicationParam):
    @sling_assets(replication=replication)
    def my_sling_assets():
        ...

    assert my_sling_assets.keys == {
        AssetKey.from_user_string(key)
        for key in [
            "public/accounts",
            "public/foo_users",
            "public/Transactions",
            "public/all_users",
            "public/finance_departments_old",
        ]
    }
