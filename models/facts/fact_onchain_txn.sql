{{ config(
    materialized='incremental',
    unique_key='transaction_id'
) }}

select
    --  Keys
    t.transaction_id,
    t.txn_hash,
    t.user_id,
    u.username,
    u.country,
    u.account_type,

    --  Transaction details
    t.txn_type,
    t.amount_num as amount,
    t.currency_norm as currency,
    t.usd_value_num as usd_value,
    t.fee_num as fee,
    t.from_address,
    t.to_address,
    t.block_number,
    t.confirmations,
    t.status_norm as status,
    t.network_norm as network,

    -- Timestamps
    t.created_at_ts as created_date,
    t.completed_date_ts as completed_date,

    -- Notes
    t.notes

from {{ ref('stg_onchain_txn') }} t
left join {{ ref('dim_user') }} u
    on t.user_id = u.user_id

{% if is_incremental() %}
  -- Only insert new rows since last run
  where t.created_date_ts > (select max(created_date) from {{ this }})
{% endif %}
