import argparse
from pathlib import Path
from datetime import datetime

from app.config import get_settings
from app.db import get_conn
from app.loader_bronze import load_invoice_xlsx_to_bronze


def _archive_file(src: Path, processed_root: Path) -> Path:
    # Archive folder: data/processed/YYYY-MM
    month_folder = datetime.now().strftime("%Y-%m")
    dest_dir = processed_root / month_folder
    dest_dir.mkdir(parents=True, exist_ok=True)

    dest = dest_dir / src.name

    # Prevent overwriting if a file with same name already exists in processed
    if dest.exists():
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dest = dest_dir / f"{src.stem}__{stamp}{src.suffix}"

    src.replace(dest)  # atomic move on same drive
    return dest


def main():
    parser = argparse.ArgumentParser(description="UPS Billing Medallion CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)

    ingest = sub.add_parser("ingest", help="Ingest UPS invoice .xlsx file(s) into Bronze")
    ingest.add_argument("--path", required=True, help="Path to a file or directory containing .xlsx files")
    ingest.add_argument(
        "--archive",
        action="store_true",
        help="Move successfully ingested files to data/processed/YYYY-MM",
    )
    ingest.add_argument(
        "--processed-dir",
        default="data/processed",
        help="Processed/archive root directory (default: data/processed)",
    )

    args = parser.parse_args()
    settings = get_settings()

    target = Path(args.path)
    if not target.exists():
        raise SystemExit(f"Path not found: {target}")

    if target.is_dir():
        files = sorted([p for p in target.glob("*.xlsx") if not p.name.startswith("~$")])
    else:
        files = [target]

    if not files:
        raise SystemExit("No .xlsx files found.")

    processed_root = Path(args.processed_dir)

    with get_conn(settings.dsn) as conn:
        for f in files:
            result = load_invoice_xlsx_to_bronze(conn, f)

            if result.load_event_id == "SKIPPED_ALREADY_SUCCESS":
                print(f"\nSkipped (already loaded): {f.name}")
                print(f"  file_id: {result.file_id}")
                continue

            print(f"\nLoaded: {f.name}")
            print(f"  file_id: {result.file_id}")
            print(f"  load_event_id: {result.load_event_id}")
            print(f"  rows_read: {result.rows_read}")
            print(f"  rows_inserted: {result.rows_inserted}")

            if args.archive:
                dest = _archive_file(f, processed_root)
                print(f"  archived_to: {dest}")


if __name__ == "__main__":
    main()

