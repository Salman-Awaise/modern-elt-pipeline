import logging
import uuid
from datetime import datetime
from pathlib import Path

from sqlalchemy import text

from modern_elt_pipeline.db import ensure_schemas, get_engine
from modern_elt_pipeline.extract.orders import extract_orders
from modern_elt_pipeline.load.postgres import load_raw_orders
from modern_elt_pipeline.logging import configure_logging
from modern_elt_pipeline.quality.raw_orders import validate_raw_orders

logger = logging.getLogger(__name__)


def ingest_raw_orders(csv_path: str = "data/raw/orders.csv") -> dict[str, int | str]:
    configure_logging()
    run_id = str(uuid.uuid4())
    started_at = datetime.utcnow()
    engine = get_engine()
    ensure_schemas(engine)

    with engine.begin() as connection:
        connection.execute(
            text(
                """
                insert into audit.pipeline_runs
                (run_id, pipeline_name, status, started_at, message)
                values (:run_id, 'orders_raw_ingestion', 'running', :started_at, 'started')
                """
            ),
            {"run_id": run_id, "started_at": started_at},
        )

    try:
        orders = extract_orders(Path(csv_path))
        rows_loaded = load_raw_orders(orders, engine)
        validate_raw_orders(engine)
        status = "success"
        message = "raw orders loaded and validated"
    except Exception as exc:
        rows_loaded = 0
        status = "failed"
        message = str(exc)
        logger.exception("Pipeline failed")
        raise
    finally:
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    update audit.pipeline_runs
                    set status = :status,
                        finished_at = :finished_at,
                        rows_extracted = :rows_extracted,
                        rows_loaded = :rows_loaded,
                        message = :message
                    where run_id = :run_id
                    """
                ),
                {
                    "run_id": run_id,
                    "status": status,
                    "finished_at": datetime.utcnow(),
                    "rows_extracted": rows_loaded,
                    "rows_loaded": rows_loaded,
                    "message": message,
                },
            )

    logger.info("Pipeline completed run_id=%s rows_loaded=%s", run_id, rows_loaded)
    return {"run_id": run_id, "rows_loaded": rows_loaded}


if __name__ == "__main__":
    ingest_raw_orders()
