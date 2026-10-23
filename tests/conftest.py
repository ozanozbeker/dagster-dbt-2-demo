"""Parse the dbt project before a test imports the definitions, which read its manifest."""

from pathlib import Path

from dagster_dbt import DbtProject

_dbt_project = DbtProject(project_dir=Path(__file__).parents[1] / "dbt")
_dbt_project.preparer.prepare(_dbt_project)
