{{ config(materialized='table') }}

with shipment_cost as (
  select *
  from {{ ref('fct_shipment_cost') }}
),

stats as (
  select
    avg(total_shipment_cost) as avg_cost,
    stddev_pop(total_shipment_cost) as std_cost
  from shipment_cost
  where total_shipment_cost is not null
),

scored as (
  select
    s.*,
    (s.total_shipment_cost - st.avg_cost) / nullif(st.std_cost, 0) as z_score
  from shipment_cost s
  cross join stats st
)

select *
from scored
where z_score >= 3
order by z_score desc, total_shipment_cost desc
