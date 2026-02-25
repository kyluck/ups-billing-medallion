{{ config(materialized='table') }}

select
  charge_description_code,
  charge_description,
  count(*) as occurrence_count,
  sum(net_amount) as total_amount,
  avg(net_amount) as avg_amount
from {{ ref('int_ups_invoice_lines_curated') }}
where tracking_number is not null
  and charge_type = 'ACCESSORIAL'
group by 1,2
order by total_amount desc

