from pathlib import Path
from typing import Iterator, List, Optional

import openpyxl


def _cell_to_str_or_none(value) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    return s if s != "" else None


def read_invoice_rows(
    xlsx_path: Path,
    expected_cols: int | None = None,
    min_non_null: int = 1,
) -> Iterator[List[Optional[str]]]:
    """
    Reads first worksheet, returns each row as list[str|None].
    Pads/truncates to expected_cols if provided.
    Filters out empty rows using min_non_null.
    """
    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    try:
        ws = wb.worksheets[0]

        for row in ws.iter_rows(values_only=True):
            values = [_cell_to_str_or_none(v) for v in row]
            if sum(1 for v in values if v is not None) < min_non_null:
                continue

            if expected_cols is not None:
                if len(values) < expected_cols:
                    values = values + [None] * (expected_cols - len(values))
                elif len(values) > expected_cols:
                    values = values[:expected_cols]

            yield values
    finally:
        wb.close()
