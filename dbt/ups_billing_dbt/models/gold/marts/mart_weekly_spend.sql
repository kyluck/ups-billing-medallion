{{ config(materialized='table') }}

with src as (
  select
    -- choose the best “time bucket” for spend:
    -- invoice_date is usually safest for finance reporting
    invoice_date,
    tracking_number,
    charge_type,
    net_amount,
    is_credit
  from {{ ref('int_ups_invoice_lines_curated') }}
  where invoice_date is not null
),

weekly as (
  select
    date_trunc('week', invoice_date)::date as week_start,

    count(*) as invoice_line_rows,
    count(distinct tracking_number) filter (where tracking_number is not null) as shipment_count,

    -- totals
    sum(net_amount) as total_net_amount,
    sum(case when is_credit then net_amount else 0 end) as total_credits,
    sum(case when not is_credit then net_amount else 0 end) as total_charges,

    -- by charge type
    sum(case when charge_type = 'BASE' then net_amount else 0 end) as base_amount,
    sum(case when charge_type = 'FUEL' then net_amount else 0 end) as fuel_amount,
    sum(case when charge_type = 'ACCESSORIAL' then net_amount else 0 end) as accessorial_amount,
    sum(case when charge_type = 'ADJUSTMENT' then net_amount else 0 end) as adjustment_amount,
    sum(case when charge_type = 'RETURN' then net_amount else 0 end) as return_amount,
    sum(case when charge_type = 'OTHER' then net_amount else 0 end) as other_amount

  from src
  group by 1
)

select *
from weekly
order by week_start desc
