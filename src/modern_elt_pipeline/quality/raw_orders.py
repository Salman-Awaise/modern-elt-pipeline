from sqlalchemy import text
from sqlalchemy.engine import Engine

from modern_elt_pipeline.config import get_settings


def validate_raw_orders(engine: Engine) -> None:
    table = f'"{get_settings().raw_schema}".orders'

    checks = {
        "raw orders must not be empty": f"select count(*) from {table}",
        "order_id must be unique": f"""
            select count(*) - count(distinct order_id)
            from {table}
        """,
        "quantity must be positive": f"""
            select count(*)
            from {table}
            where quantity <= 0
        """,
        "unit_price must be non-negative": f"""
            select count(*)
            from {table}
            where unit_price < 0
        """,
    }

    with engine.begin() as connection:
        row_count = connection.execute(text(checks["raw orders must not be empty"])).scalar_one()
        if row_count == 0:
            raise ValueError(f"{table} is empty")

        for name, query in checks.items():
            if name == "raw orders must not be empty":
                continue
            failures = connection.execute(text(query)).scalar_one()
            if failures:
                raise ValueError(f"{name}: {failures} failing rows")
