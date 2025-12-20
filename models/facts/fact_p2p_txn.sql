{{ config(
    materialized='incremental',
    unique_key='txn_id'
) }}

select
    -- 🔑 Keys
    t.txn_id,
    t.sender_id,
    s.username as sender_username,
    s.country as sender_country,
    t.receiver_id,
    r.username as receiver_username,
    r.country as receiver_country,

    -- 💰 Transaction details
    t.amount_num as amount,
    t.currency_norm as currency,
    t.fee_num as fee,
    t.status_norm as status,

    -- 🕒 Timestamps
    t.created_at_ts as created_at,
    t.completed_at_ts as completed_at

from {{ ref('stg_p2p_transactions') }} t
left join {{ ref('dim_user') }} s
    on t.sender_id = s.user_id
left join {{ ref('dim_user') }} r
    on t.receiver_id = r.user_id

{% if is_incremental() %}
  -- Only insert new rows since last run
  where t.created_at_ts > (select max(created_at) from {{ this }})
{% endif %}
