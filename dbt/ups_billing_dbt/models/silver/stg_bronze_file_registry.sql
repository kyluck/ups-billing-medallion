select
  file_id,
  original_filename,
  file_hash_sha256,
  file_size_bytes,
  detected_billing_period,
  first_seen_at,
  notes
from {{ source('bronze', 'file_registry') }}
