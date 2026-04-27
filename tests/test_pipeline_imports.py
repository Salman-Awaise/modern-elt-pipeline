from modern_elt_pipeline.pipeline import ingest_raw_orders


def test_pipeline_entrypoint_is_importable() -> None:
    assert callable(ingest_raw_orders)
