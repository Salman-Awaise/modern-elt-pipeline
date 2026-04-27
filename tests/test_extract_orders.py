from pathlib import Path

import pytest

from modern_elt_pipeline.extract.orders import extract_orders


def test_extract_orders_requires_expected_columns(tmp_path: Path) -> None:
    csv_path = tmp_path / "bad_orders.csv"
    csv_path.write_text("order_id,customer_id\n1,2\n")

    with pytest.raises(ValueError, match="missing required columns"):
        extract_orders(csv_path)
