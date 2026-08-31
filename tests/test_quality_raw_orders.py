import pytest

from modern_elt_pipeline.config import get_settings
from modern_elt_pipeline.quality import raw_orders


class _FakeResult:
    def __init__(self, value: int) -> None:
        self._value = value

    def scalar_one(self) -> int:
        return self._value


class _FakeConnection:
    """Records the SQL it is handed and returns a canned count for each query."""

    def __init__(self, counts: list[int]) -> None:
        self.counts = list(counts)
        self.statements: list[str] = []

    def execute(self, statement, *args, **kwargs) -> _FakeResult:
        self.statements.append(str(statement))
        return _FakeResult(self.counts.pop(0))

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        return None


class _FakeEngine:
    def __init__(self, connection: _FakeConnection) -> None:
        self._connection = connection

    def begin(self) -> _FakeConnection:
        return self._connection


def test_checks_target_the_configured_raw_schema(monkeypatch) -> None:
    monkeypatch.setenv("RAW_SCHEMA", "landing")
    get_settings.cache_clear()

    connection = _FakeConnection([5, 0, 0, 0])
    raw_orders.validate_raw_orders(_FakeEngine(connection))

    get_settings.cache_clear()
    assert connection.statements, "no queries were issued"
    assert all('"landing".orders' in s for s in connection.statements)
    assert not any("from raw.orders" in s for s in connection.statements)


def test_empty_table_raises() -> None:
    connection = _FakeConnection([0])
    with pytest.raises(ValueError, match="is empty"):
        raw_orders.validate_raw_orders(_FakeEngine(connection))


def test_failing_check_raises_with_row_count() -> None:
    # non-empty, but duplicate order_ids
    connection = _FakeConnection([5, 2, 0, 0])
    with pytest.raises(ValueError, match="order_id must be unique: 2 failing rows"):
        raw_orders.validate_raw_orders(_FakeEngine(connection))


def test_clean_data_passes() -> None:
    connection = _FakeConnection([5, 0, 0, 0])
    raw_orders.validate_raw_orders(_FakeEngine(connection))
