from sqlalchemy import text
from sqlalchemy.engine import Engine


def validate_raw_orders(engine: Engine) -> None:
    checks = {
        "raw orders must not be empty": "select count(*) from raw.orders",
        "order_id must be unique": """
            select count(*) - count(distinct order_id)
            from raw.orders
        """,
        "quantity must be positive": """
            select count(*)
            from raw.orders
            where quantity <= 0
        """,
        "unit_price must be non-negative": """
            select count(*)
            from raw.orders
            where unit_price < 0
        """,
    }

    with engine.begin() as connection:
        row_count = connection.execute(text(checks["raw orders must not be empty"])).scalar_one()
        if row_count == 0:
            raise ValueError("raw.orders is empty")

        for name, query in checks.items():
            if name == "raw orders must not be empty":
                continue
            failures = connection.execute(text(query)).scalar_one()
            if failures:
                raise ValueError(f"{name}: {failures} failing rows")
