{{ config(materialized='table') }}

with lines as (
  select *
  from {{ ref('int_ups_invoice_lines_curated') }}
),

by_load as (
  select
    load_event_id,
    file_id,
    min(ingested_at) as first_ingested_at,
    max(ingested_at) as last_ingested_at,

    count(*) as invoice_line_rows,

    count(*) filter (where tracking_number is not null) as shipment_lines,
    count(*) filter (where tracking_number is null) as non_shipment_lines,

    count(*) filter (where is_credit = true) as credit_lines,

    min(invoice_date) as min_invoice_date,
    max(invoice_date) as max_invoice_date,

    sum(net_amount) as total_net_amount,
    sum(case when is_credit then net_amount else 0 end) as total_credits,
    sum(case when not is_credit then net_amount else 0 end) as total_charges

  from lines
  group by 1,2
)

select *
from by_load
order by last_ingested_at desc
