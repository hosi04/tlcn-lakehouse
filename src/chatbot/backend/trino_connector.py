from __future__ import annotations

import os
import logging

from trino.dbapi import connect
import pandas as pd

logger = logging.getLogger(__name__)


def _get_connection():
    host    = os.getenv("TRINO_HOST", "localhost")
    port    = int(os.getenv("TRINO_PORT", "8085"))
    user    = os.getenv("TRINO_USER", "admin")
    catalog = os.getenv("TRINO_CATALOG", "iceberg")
    schema  = os.getenv("TRINO_SCHEMA", "gold")
    return connect(host=host, port=port, user=user, catalog=catalog, schema=schema)


def trino_query(sql: str) -> pd.DataFrame:
    conn = _get_connection()
    cur  = conn.cursor()
    try:
        cur.execute(sql)
        rows    = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        return pd.DataFrame(rows, columns=columns)
    except Exception:
        logger.exception("Trino query failed: %s", sql[:200])
        raise
    finally:
        cur.close()
        conn.close()
