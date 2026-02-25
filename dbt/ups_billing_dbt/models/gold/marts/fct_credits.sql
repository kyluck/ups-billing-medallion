{{ config(materialized='table') }}

select
  invoice_date,
  transaction_date,
  invoice_number,
  account_number,
  tracking_number,
  charge_description,
  net_amount as credit_amount
from {{ ref('int_ups_invoice_lines_curated') }}
where is_credit = true
order by invoice_date desc, transaction_date desc
