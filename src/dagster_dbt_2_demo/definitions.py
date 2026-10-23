"""Dagster definitions for the demo dbt project, which dbt 2.0 runs through the dagster-dbt fork.

Neither `@dbt_assets` passes `project=`, so the fork evaluates `select` and `selector` against the
manifest in Python. Upstream dagster-dbt raises there unless dbt-core is installed.
"""

from pathlib import Path

import dagster as dg
from dagster_dbt import DbtCliResource, DbtProject, build_schedule_from_dbt_selection, dbt_assets

dbt_project = DbtProject(project_dir=Path(__file__).parents[2] / "dbt")
dbt_project.prepare_if_dev()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    select="resource_type:seed path:models/staging",
    name="staging",
)
def staging(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    """Build the seeds and staging models, with row counts and column metadata."""
    yield from (
        dbt.cli(["build"], context=context).stream().fetch_row_counts().fetch_column_metadata()
    )


@dbt_assets(manifest=dbt_project.manifest_path, selector="marts", name="marts")
def marts(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    """Build the marts and snapshots that `selectors.yml` names, with their unit test."""
    yield from (
        dbt.cli(["build"], context=context).stream().fetch_row_counts().fetch_column_metadata()
    )


daily_marts = build_schedule_from_dbt_selection(
    [marts],
    job_name="daily_marts",
    cron_schedule="0 6 * * *",
    dbt_select="tag:mart",
)

defs = dg.Definitions(
    assets=[staging, marts],
    schedules=[daily_marts],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
)
