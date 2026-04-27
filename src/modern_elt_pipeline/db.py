from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from modern_elt_pipeline.config import get_settings


def get_engine() -> Engine:
    return create_engine(get_settings().sqlalchemy_url, pool_pre_ping=True)


def ensure_schemas(engine: Engine) -> None:
    settings = get_settings()
    with engine.begin() as connection:
        connection.execute(text(f'create schema if not exists "{settings.raw_schema}"'))
        connection.execute(text(f'create schema if not exists "{settings.dbt_schema}"'))
        connection.execute(text("create schema if not exists audit"))
        connection.execute(
            text(
                """
                create table if not exists audit.pipeline_runs (
                    run_id text primary key,
                    pipeline_name text not null,
                    status text not null,
                    started_at timestamp not null,
                    finished_at timestamp,
                    rows_extracted integer default 0,
                    rows_loaded integer default 0,
                    message text
                )
                """
            )
        )
