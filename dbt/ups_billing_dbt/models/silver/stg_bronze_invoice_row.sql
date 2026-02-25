select
  bronze_row_id,
  file_id,
  load_event_id,
  row_number,
  raw_values,
  jsonb_array_length(raw_values) as raw_values_len,
  ingested_at
from {{ source('bronze', 'invoice_row') }}
