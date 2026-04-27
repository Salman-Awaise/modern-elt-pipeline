from pathlib import Path

import pandas as pd


REQUIRED_COLUMNS = {
    "order_id",
    "customer_id",
    "order_date",
    "status",
    "quantity",
    "unit_price",
    "country",
}


def extract_orders(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    missing = REQUIRED_COLUMNS.difference(frame.columns)
    if missing:
        raise ValueError(f"Input file is missing required columns: {sorted(missing)}")
    return frame
