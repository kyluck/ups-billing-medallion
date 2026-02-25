select
  load_event_id,
  file_id,
  started_at,
  finished_at,
  status,
  rows_read,
  rows_inserted,
  error_message,
  loader_version
from {{ source('bronze', 'load_event') }}
