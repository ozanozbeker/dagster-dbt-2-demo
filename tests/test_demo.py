"""One test per dagster-dbt fix the demo shows, run with dbt 2.0 and no dbt-core installed."""

import importlib.metadata
import json
import shutil
import sys
import warnings
from pathlib import Path
from typing import Any

import dagster as dg
import pytest
from dagster_dbt import DbtCliResource
from dagster_dbt.compat import _warn_if_dbt_2_and_dbt_core_installed
from dagster_dbt.core.resource import DBT_EXECUTABLE

from dagster_dbt_2_demo.definitions import daily_marts, dbt_project, defs, marts, staging


@pytest.fixture(name="materializations", scope="module")
def materializations_fixture() -> dict[str, dict[str, Any]]:
    result = dg.materialize(
        [staging, marts], resources={"dbt": DbtCliResource(project_dir=dbt_project)}
    )
    assert result.success
    return {
        event.asset_key.to_user_string(): event.event_specific_data.materialization.metadata
        for event in result.get_asset_materialization_events()
    }


def test_dbt_2_runs_without_dbt_core() -> None:
    """dbt 2.0 is installed alone, and dagster-dbt imports without dbt-core or `python-dateutil`
    pinned by hand.
    """
    assert importlib.metadata.version("dbt").startswith("2.")
    with pytest.raises(importlib.metadata.PackageNotFoundError):
        importlib.metadata.version("dbt-core")
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        _warn_if_dbt_2_and_dbt_core_installed()


def test_selection_without_project() -> None:
    """`select`, `selector` and a schedule's `dbt_select` resolve from the manifest alone."""
    assert {key.to_user_string() for key in staging.keys} == {
        "raw_customers",
        "raw_orders",
        "stg_customers",
        "stg_orders",
        "stg_events",
    }
    assert {key.to_user_string() for key in marts.keys} == {
        "customer_totals",
        "customers_snapshot",
    }
    job = defs.resolve_job_def(daily_marts.job_name)
    assert {key.to_user_string() for key in job.asset_layer.executable_asset_keys} == {
        "customer_totals"
    }


def test_environment_dbt_runs() -> None:
    """The `dbt` that runs is the one in this environment, not another on PATH."""
    resolved = Path(shutil.which(DBT_EXECUTABLE) or DBT_EXECUTABLE).resolve()

    assert resolved.is_relative_to(Path(sys.prefix).resolve())


def test_unit_test_runs_without_keyerror(materializations: dict[str, dict[str, Any]]) -> None:
    """The run builds a model with a dbt unit test and still materializes it."""
    manifest = json.loads(dbt_project.manifest_path.read_text())

    assert manifest["unit_tests"]
    assert "customer_totals" in materializations


def test_seeds_keep_materialization_type() -> None:
    """dbt 2.0 leaves `config.materialized` empty for seeds, and the asset still says "seed"."""
    spec = next(spec for spec in staging.specs if spec.key == dg.AssetKey("raw_customers"))

    assert spec.metadata["dagster-dbt/materialization_type"] == "seed"


def test_row_counts(materializations: dict[str, dict[str, Any]]) -> None:
    """`fetch_row_counts()` attaches a row count to every table."""
    row_counts = {
        key: metadata["dagster/row_count"].value for key, metadata in materializations.items()
    }

    assert row_counts == {
        "raw_customers": 3,
        "raw_orders": 3,
        "stg_customers": 3,
        "stg_orders": 3,
        "stg_events": 3,
        "customer_totals": 3,
        "customers_snapshot": 3,
    }


def test_column_metadata_and_lineage(materializations: dict[str, dict[str, Any]]) -> None:
    """`fetch_column_metadata()` attaches column schemas and column lineage."""
    metadata = materializations["customer_totals"]
    schema = metadata["dagster/column_schema"].value
    lineage = metadata["dagster/column_lineage"].value

    assert [column.name for column in schema.columns] == [
        "customer_id",
        "first_name",
        "total_amount",
        "event_count",
    ]
    assert {
        (dep.asset_key.to_user_string(), dep.column_name)
        for dep in lineage.deps_by_column["total_amount"]
    } == {("stg_orders", "amount")}
