{{ config(materialized='table') }}

with base as (
  select *
  from {{ ref('int_ups_invoice_lines_curated') }}
  where tracking_number is not null
),

dupes as (
  select
    invoice_number,
    account_number,
    tracking_number,
    transaction_date,
    charge_description_code,
    charge_description,
    net_amount,
    count(*) as dup_count,
    sum(net_amount) as dup_total_amount
  from base
  group by 1,2,3,4,5,6,7
  having count(*) > 1
)

select *
from dupes
order by dup_total_amount desc, dup_count desc
