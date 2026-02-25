{{ config(materialized='table') }}

with base as (
  select *
  from {{ ref('int_ups_invoice_lines_curated') }}
  where tracking_number is not null
)

select
  tracking_number,
  min(invoice_date) as invoice_date,
  min(invoice_number) as invoice_number,
  min(account_number) as account_number,
  sum(net_amount) as total_shipment_cost,
  sum(case when is_credit then net_amount else 0 end) as total_credits,
  sum(case when not is_credit then net_amount else 0 end) as total_charges,
  count(*) as line_count
from base
group by 1
