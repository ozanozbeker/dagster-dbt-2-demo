# dagster-dbt with dbt 2.0

This repo runs a small dbt project with dbt 2.0 under Dagster. dagster-dbt 0.29.26 does not support dbt 2.0, so the repo installs [a fork of dagster-dbt](https://github.com/ozanozbeker/dagster-dbt) that fixes what breaks. Each fix has a test in `tests/test_demo.py`, so you can check it on your own machine in a minute.

The fork is temporary. Once upstream dagster-dbt supports dbt 2.0, switch back to the PyPI release.

## Run it

You need [uv](https://docs.astral.sh/uv/). Everything else, including dbt 2.0 and DuckDB, installs into the project's virtual environment:

```bash
git clone https://github.com/ozanozbeker/dagster-dbt-2-demo.git
cd dagster-dbt-2-demo
uv sync
```

Run the tests. They build the whole dbt project through Dagster and check every fix:

```bash
uv run pytest
```

Then open the Dagster UI, select every asset and materialize them:

```bash
uv run dagster dev
```

Each materialization shows a row count, a column schema and, for models, column lineage. `customer_totals` also runs its dbt unit test.

## What breaks, and what the fork changes

The table maps each problem with upstream dagster-dbt 0.29.26 and dbt 2.0.9 to the test that shows the fork's fix.

| Upstream with dbt 2.0 | Fork | Test |
| --- | --- | --- |
| `import dagster_dbt` fails: `No module named 'dateutil'`. Only dbt-core brought it in. | Declares `python-dateutil`. | `test_dbt_2_runs_without_dbt_core` |
| A passing dbt unit test fails the step with a `KeyError`. | Skips unit tests and functions when it maps results to assets. | `test_unit_test_runs_without_keyerror` |
| `@dbt_assets(select=...)` without `project=` raises and asks for dbt-core. | Evaluates `select`, `exclude` and `selector` against the manifest. | `test_selection_without_project` |
| `@dbt_assets(selector=...)` runs `dbt build --select fqn:* --selector ...`, which dbt 2.0 rejects. | Sends `--selector` alone. | `test_row_counts` (the `marts` group) |
| `fetch_row_counts()` and `fetch_column_metadata()` log a warning and add nothing. | Runs one `dbt show` and one `dbt compile --write-catalog` after dbt exits. | `test_row_counts`, `test_column_metadata_and_lineage` |
| Seeds lose `dagster-dbt/materialization_type`. | Falls back to `seed`. | `test_seeds_keep_materialization_type` |
| An unactivated virtualenv runs whichever `dbt` comes first on `PATH`. | Runs the `dbt` installed next to dagster-dbt. | `test_environment_dbt_runs` |
| dbt 2.0 and dbt-core in one environment overwrite each other's files without a word. | Warns at import. | `test_dbt_2_runs_without_dbt_core` |

## Use it in your own project

Add the fork and dbt 2.0, pinning the fork to a tag:

```bash
uv add "dagster-dbt @ git+https://github.com/ozanozbeker/dagster-dbt@v0.29.26-fusion.3" dbt
```

Do not install dbt-core or any `dbt-<adapter>` package beside it. dbt 2.0 ships its adapters, and dbt-core writes files into the same `dbt` package. If both are installed, `import dagster_dbt` warns.

`src/dagster_dbt_2_demo/definitions.py` shows the patterns:

- `@dbt_assets` with `select` or `selector`, and no `project=`.
- `.stream().fetch_row_counts().fetch_column_metadata()` on every build.
- `build_schedule_from_dbt_selection` with a dbt selection string.

## What to expect

The fork works fully with dbt 2.0, with these differences from dbt-core:

- With `fetch_row_counts()` or `fetch_column_metadata()`, materializations appear when dbt finishes, not one at a time, because the metadata comes from one batch after the run.
- Column types use the warehouse's own names, such as `VARCHAR` where dbt-core's DuckDB adapter reports `character varying(256)`.
- Selection without `project=` cannot use `state:`, `result:` or `source_status:`, because those read dbt run artifacts. Pass `project=` for them.

The fork has only been tested on DuckDB. Snowflake, BigQuery and Windows have not been tried, so check row counts and column metadata there before you rely on them.

## Layout

The repo has three parts:

- `dbt/` holds the dbt project: two seeds, a source that `on-run-start` creates, three staging models (one incremental), a mart with a unit test, a YAML snapshot, an exposure and a `selectors.yml`.
- `src/dagster_dbt_2_demo/definitions.py` holds the Dagster assets, the resource and a schedule.
- `tests/test_demo.py` holds one test per fix.
