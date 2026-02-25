with rows as (
  select
    bronze_row_id,
    file_id,
    load_event_id,
    row_number,
    ingested_at,
    raw_values
  from {{ ref('stg_bronze_invoice_row') }}
)
select
  r.bronze_row_id,
  r.file_id,
  r.load_event_id,
  r.row_number,
  r.ingested_at,
  (c.ordinality)::int as col_index,
  nullif(trim(c.value), '') as cell_value
from rows r
cross join lateral jsonb_array_elements_text(r.raw_values) with ordinality as c(value, ordinality)
