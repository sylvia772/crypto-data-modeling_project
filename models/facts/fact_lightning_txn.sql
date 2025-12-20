{{ config(
    materialized='incremental',
    unique_key='lightning_id'
) }}

select
    -- 🔑 Keys
    t.lightning_id,
    t.payment_hash,
    t.user_id,
    u.username,
    u.country,
    u.account_type,

    -- 🔄 Transaction details
    t.txn_direction_norm as txn_direction,
    t.amount_sats_int as amount_sats,
    t.amount_btc_num as amount_btc,
    t.fee_sats_int as fee_sats,
    t.status_norm as status,
    t.payment_request,
    t.destination,
    t.route_hints,
    t.is_keysend_bool as is_keysend,
    t.preimage,
    t.memo,

    -- 🕒 Timestamps
    t.txn_timestamp,
    t.settled_at_ts,
    t.expiry_ts,
    t.created_ts

from {{ ref('stg_lightning_txn') }} t
left join {{ ref('dim_user') }} u
    on t.user_id = u.user_id

{% if is_incremental() %}
  -- Only insert new rows since last run
  where t.created_ts > (select max(created_ts) from {{ this }})
{% endif %}
