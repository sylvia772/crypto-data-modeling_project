{{ config(
    materialized='incremental',
    unique_key='vas_id'
) }}

select
    -- 🔑 Keys
    t.vas_id,
    t.user_id,
    u.username,
    u.country,
    u.account_type,

    -- 🛠 Service details
    t.service_type_norm as service_type,
    t.provider_norm as provider,
    t.product_code,
    t.recipient_number,

    -- 💰 Transaction details
    t.amount_num as amount,
    t.currency_norm as currency,
    t.commission_num as commission,
    t.retry_count_int as retry_count,
    t.status_norm as status,

    -- 🕒 Timestamps
    t.transaction_date_ts as transaction_date,
    t.completed_date_ts as completed_date,
    t.created_at_ts as created_at,

    -- 🔗 External reference
    t.external_reference,

    -- 📝 Error and API response
    t.error_message,
    t.api_response

from {{ ref('stg_vas_transaction') }} t
left join {{ ref('dim_user') }} u
   on t.user_id = u.user_id

{% if is_incremental() %}
  -- Only insert new rows since last run
  where t.created_at_ts > (select max(created_at) from {{ this }})
{% endif %}
