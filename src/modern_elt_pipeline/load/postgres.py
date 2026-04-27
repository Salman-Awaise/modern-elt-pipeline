import logging

import pandas as pd
from sqlalchemy.engine import Engine

from modern_elt_pipeline.config import get_settings

logger = logging.getLogger(__name__)


def load_raw_orders(frame: pd.DataFrame, engine: Engine) -> int:
    settings = get_settings()
    frame.to_sql(
        "orders",
        con=engine,
        schema=settings.raw_schema,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1000,
    )
    logger.info("Loaded raw.orders rows=%s", len(frame))
    return len(frame)
