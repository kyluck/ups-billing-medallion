from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple
import json

from tqdm import tqdm

from app.db import execute, fetchone, insert_many_values
from app.hashing import sha256_file
from app.excel_reader import read_invoice_rows


@dataclass
class LoadResult:
    file_id: str
    load_event_id: str
    rows_read: int
    rows_inserted: int


def _detect_billing_period_from_filename(name: str) -> Optional[str]:
    # v1 heuristic: last underscore piece like 082325 or 20250823
    stem = Path(name).stem
    parts = stem.split("_")
    if not parts:
        return None
    maybe = parts[-1]
    if maybe.isdigit() and len(maybe) in (6, 8):
        return maybe
    return None


def upsert_file_registry(conn, file_path: Path) -> Tuple[str, bool]:
    file_hash = sha256_file(file_path)
    file_size = file_path.stat().st_size
    billing_period = _detect_billing_period_from_filename(file_path.name)

    existing = fetchone(
        conn,
        "SELECT file_id FROM bronze.file_registry WHERE file_hash_sha256 = %s",
        (file_hash,),
    )
    if existing:
        return str(existing[0]), False

    row = fetchone(
        conn,
        """
        INSERT INTO bronze.file_registry
          (original_filename, file_hash_sha256, file_size_bytes, detected_billing_period)
        VALUES (%s, %s, %s, %s)
        RETURNING file_id
        """,
        (file_path.name, file_hash, file_size, billing_period),
    )
    return str(row[0]), True


def file_already_loaded_success(conn, file_id: str) -> bool:
    row = fetchone(
        conn,
        """
        SELECT 1
        FROM bronze.load_event
        WHERE file_id = %s AND status = 'SUCCESS'
        LIMIT 1
        """,
        (file_id,),
    )
    return row is not None


def create_load_event(conn, file_id: str, loader_version: str = "v1") -> str:
    row = fetchone(
        conn,
        """
        INSERT INTO bronze.load_event (file_id, status, loader_version)
        VALUES (%s, 'STARTED', %s)
        RETURNING load_event_id
        """,
        (file_id, loader_version),
    )
    return str(row[0])


def finalize_load_event_success(conn, load_event_id: str, rows_read: int, rows_inserted: int) -> None:
    execute(
        conn,
        """
        UPDATE bronze.load_event
        SET status='SUCCESS',
            finished_at=now(),
            rows_read=%s,
            rows_inserted=%s,
            error_message=NULL
        WHERE load_event_id=%s
        """,
        (rows_read, rows_inserted, load_event_id),
    )


def finalize_load_event_failed(conn, load_event_id: str, rows_read: int, rows_inserted: int, error_message: str) -> None:
    execute(
        conn,
        """
        UPDATE bronze.load_event
        SET status='FAILED',
            finished_at=now(),
            rows_read=%s,
            rows_inserted=%s,
            error_message=%s
        WHERE load_event_id=%s
        """,
        (rows_read, rows_inserted, error_message[:5000], load_event_id),
    )


def load_invoice_xlsx_to_bronze(
    conn,
    xlsx_path: Path,
    expected_cols: int = 244,
    batch_size: int = 1000,
) -> LoadResult:
    file_id, _ = upsert_file_registry(conn, xlsx_path)

    # Guardrail: prevent accidental double-ingestion of the exact same file
    if file_already_loaded_success(conn, file_id):
        return LoadResult(
            file_id=file_id,
            load_event_id="SKIPPED_ALREADY_SUCCESS",
            rows_read=0,
            rows_inserted=0,
        )

    load_event_id = create_load_event(conn, file_id)

    rows_read = 0
    rows_inserted = 0

    insert_sql = """
        INSERT INTO bronze.invoice_row
          (file_id, load_event_id, row_number, raw_values)
        VALUES %s
        ON CONFLICT (load_event_id, row_number) DO NOTHING
    """

    try:
        buffer = []
        for i, values in enumerate(
            tqdm(read_invoice_rows(xlsx_path, expected_cols=expected_cols), desc=f"Loading {xlsx_path.name}"),
            start=1,
        ):
            rows_read += 1
            buffer.append((file_id, load_event_id, i, json.dumps(values)))

            if len(buffer) >= batch_size:
                insert_many_values(conn, insert_sql, buffer, page_size=batch_size)
                rows_inserted += len(buffer)
                buffer.clear()

        if buffer:
            insert_many_values(conn, insert_sql, buffer, page_size=batch_size)
            rows_inserted += len(buffer)

        finalize_load_event_success(conn, load_event_id, rows_read, rows_inserted)
        return LoadResult(file_id=file_id, load_event_id=load_event_id, rows_read=rows_read, rows_inserted=rows_inserted)

    except Exception as e:
        finalize_load_event_failed(conn, load_event_id, rows_read, rows_inserted, str(e))
        raise

