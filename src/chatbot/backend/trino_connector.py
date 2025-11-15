from trino.dbapi import connect
import os
import pandas as pd

def trino_query(sql: str):
    """
    Query Trino từ Python.
    """
    host = os.getenv("TRINO_HOST")
    if not host:
        host = "localhost"

    port = int(os.getenv("TRINO_PORT", 8080))
    user = os.getenv("TRINO_USER", "admin")
    catalog = os.getenv("TRINO_CATALOG", "iceberg")
    schema = os.getenv("TRINO_SCHEMA", "gold")

    conn = connect(
        host=host,
        port=port,
        user=user,
        catalog=catalog,
        schema=schema
    )
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()

    columns = [desc[0] for desc in cur.description]
    df = pd.DataFrame(rows, columns=columns)
    return df
