{{ config(materialized='table') }}

with src as (
  select *
  from {{ ref('stg_ups_invoice_lines') }}
),

typed as (
  select
    -- lineage / audit
    bronze_row_id,
    file_id,
    load_event_id,
    row_number,
    ingested_at,

    -- identifiers
    invoice_number,
    account_number,
    tracking_number,

    -- classify lines
    case
      when tracking_number is null then 'non_shipment_line'
      else 'shipment_line'
    end as line_type,


    -- references commonly used for matching
    shipment_reference_number_1,
    shipment_reference_number_2,
    package_reference_number_1,
    package_reference_number_2,
    customer_reference_number,

    -- dates (cast once)
    nullif(invoice_date, '')::date             as invoice_date,
    nullif(transaction_date, '')::date         as transaction_date,
    nullif(shipment_date, '')::date            as shipment_date,
    nullif(shipment_delivery_date, '')::date   as delivery_date,

    -- service / rating context
    zone,
    charge_category_code,
    charge_category_detail_code,
    charge_description_code,
    charge_description,
    charge_classification_code,

    -- weights / qty (cast once)
    nullif(package_quantity, '')::int          as package_quantity,
    nullif(entered_weight, '')::numeric        as entered_weight,
    entered_weight_unit_of_measure,
    nullif(billed_weight, '')::numeric         as billed_weight,
    billed_weight_unit_of_measure,

    -- money (cast once)
    invoice_currency_code,
    transaction_currency_code,
    nullif(invoice_amount, '')::numeric        as invoice_amount,
    nullif(incentive_amount, '')::numeric      as incentive_amount,
    nullif(net_amount, '')::numeric            as net_amount

  from src
),

final as (
  select
    *,
    case
      when net_amount is null then null
      when net_amount < 0 then true
      else false
    end as is_credit,

    case
      -- base transportation
      when charge_category_code = 'SHP'
       and charge_category_detail_code in ('MAN','WWS')
       and charge_description_code in ('1','2','3','12')
        then 'BASE'

      -- fuel
      when charge_description_code = 'FSC'
        then 'FUEL'

      -- accessorials / surcharges
      when charge_description_code in (
        'RES','HRS',
        'RDR','RDC','LDR','LDC',
        'DSR','SUI',
        'CSG',
        'CAR',
        'PFC','PFR'
      )
        then 'ACCESSORIAL'

      -- adjustments / corrections
      when charge_category_code = 'ADJ'
        then 'ADJUSTMENT'

      -- returns
      when charge_category_code = 'RTN'
        then 'RETURN'

      else 'OTHER'
    end as charge_type

  from typed
)


select * from final
