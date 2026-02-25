import csv
import re
from pathlib import Path

HEADER_PATH = Path("data/headers/UPS_billing_data_header.csv")
OUT_PATH = Path("dbt/ups_billing_dbt/models/silver/stg_ups_invoice_lines.sql")
N_COLS = 244  # your data min/max cols = 244

def snake(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        s = "col"
    if s[0].isdigit():
        s = f"col_{s}"
    return s

def dedupe(names):
    seen = {}
    out = []
    for n in names:
        if n not in seen:
            seen[n] = 1
            out.append(n)
        else:
            seen[n] += 1
            out.append(f"{n}_{seen[n]}")
    return out

def main():
    with HEADER_PATH.open(newline="", encoding="utf-8-sig") as f:
        headers = next(csv.reader(f))

    if len(headers) < N_COLS:
        raise SystemExit(f"Header has {len(headers)} columns, expected at least {N_COLS}.")

    headers_244 = headers[:N_COLS]
    col_names = dedupe([snake(h) for h in headers_244])

    sql = []
    sql.append("{{ config(materialized='table') }}\n\n")
    sql.append("with src as (\n")
    sql.append("  select\n")
    sql.append("    bronze_row_id,\n")
    sql.append("    file_id,\n")
    sql.append("    load_event_id,\n")
    sql.append("    row_number,\n")
    sql.append("    raw_values,\n")
    sql.append("    ingested_at\n")
    sql.append("  from {{ ref('stg_bronze_invoice_row') }}\n")
    sql.append(")\n\n")
    sql.append("select\n")
    sql.append("  bronze_row_id,\n")
    sql.append("  file_id,\n")
    sql.append("  load_event_id,\n")
    sql.append("  row_number,\n")
    sql.append("  ingested_at,\n")

    for i, name in enumerate(col_names):
        comma = "," if i < len(col_names) - 1 else ""
        sql.append(f"  nullif(trim(raw_values->>{i}), '') as {name}{comma}\n")

    sql.append("from src\n")

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text("".join(sql), encoding="utf-8")

    print(f"Wrote: {OUT_PATH}")
    print(f"Header columns in file: {len(headers)}")
    print(f"Columns used in model: {N_COLS}")

if __name__ == "__main__":
    main()
