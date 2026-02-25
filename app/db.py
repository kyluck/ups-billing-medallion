from contextlib import contextmanager
from typing import Any, Sequence, Tuple

import psycopg2
from psycopg2.extras import execute_values


@contextmanager
def get_conn(dsn: str):
    conn = psycopg2.connect(dsn)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def fetchone(conn, sql: str, params: Tuple[Any, ...]):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()


def execute(conn, sql: str, params: Tuple[Any, ...] | None = None) -> None:
    with conn.cursor() as cur:
        cur.execute(sql, params)


def insert_many_values(
    conn,
    sql: str,
    rows: Sequence[Sequence[Any]],
    page_size: int = 1000,
) -> None:
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=page_size)
