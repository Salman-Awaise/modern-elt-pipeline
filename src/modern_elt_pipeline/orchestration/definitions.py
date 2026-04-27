import os
import subprocess
from pathlib import Path

from dagster import AssetSelection, Definitions, ScheduleDefinition, asset, define_asset_job

from modern_elt_pipeline.pipeline import ingest_raw_orders


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DBT_DIR = PROJECT_ROOT / "dbt"


@asset(group_name="ingestion")
def raw_orders() -> dict:
    return ingest_raw_orders(str(PROJECT_ROOT / "data" / "raw" / "orders.csv"))


@asset(deps=[raw_orders], group_name="transformation")
def dbt_transformations() -> None:
    env = os.environ.copy()
    env.setdefault("POSTGRES_HOST", "postgres")
    env.setdefault("POSTGRES_PORT", "5432")
    env.setdefault("POSTGRES_DB", "analytics")
    env.setdefault("POSTGRES_USER", "analytics")
    env.setdefault("POSTGRES_PASSWORD", "analytics")
    env.setdefault("DBT_PROFILES_DIR", str(DBT_DIR))

    subprocess.run(
        ["dbt", "build", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)],
        check=True,
        env=env,
    )


elt_job = define_asset_job("orders_elt_job", selection=AssetSelection.all())

daily_orders_schedule = ScheduleDefinition(
    job=elt_job,
    cron_schedule="0 8 * * *",
)


defs = Definitions(
    assets=[raw_orders, dbt_transformations],
    jobs=[elt_job],
    schedules=[daily_orders_schedule],
)
