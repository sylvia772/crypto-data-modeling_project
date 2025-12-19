{{ config(materialized='view') }}

select
    -- keys
    trim(txn_id) as txn_id,
    trim(sender_id) as sender_id,
    trim(receiver_id) as receiver_id,

    -- amounts (remove letters, symbols, commas)
    case
        when amount is null then null
        else regexp_replace(trim(amount), '[^0-9.]', '', 'g')::numeric
    end as amount_num,

    case
        when exchange_rate is null then null
        else regexp_replace(trim(exchange_rate), '[^0-9.]', '', 'g')::numeric
    end as exchange_rate_num,

    case
        when fee is null then null
        else regexp_replace(trim(fee), '[^0-9.]', '', 'g')::numeric
    end as fee_num,

    -- currency normalization
    case
        when lower(trim(currency)) in ('ngn','naira') then 'NGN'
        when lower(trim(currency)) = 'usd' then 'USD'
        else upper(trim(currency))
    end as currency_norm,

    -- fee bearer normalization
    case
        when lower(trim(fee_bearer)) = 'sender' then 'sender'
        when lower(trim(fee_bearer)) = 'receiver' then 'receiver'
        when lower(trim(fee_bearer)) = 'split' then 'split'
        else lower(trim(fee_bearer))
    end as fee_bearer_norm,

    -- status normalization
    case
        when lower(trim(status)) in ('completed','1','true') then 'completed'
        when lower(trim(status)) = 'pending' then 'pending'
        when lower(trim(status)) in ('cancelled','failed','0','false') then 'cancelled'
        else lower(trim(status))
    end as status_norm,

    -- payment method normalization
    case
        when lower(trim(payment_method)) = 'wallet' then 'wallet'
        when lower(trim(payment_method)) in ('bank_transfer','bank') then 'bank_transfer'
        else lower(trim(payment_method))
    end as payment_method_norm,

    -- reference and description
    trim(reference_number) as reference_number,
    description,

    -- timestamps safely parsed
    case when transaction_date ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$' 
        then to_timestamp(transaction_date, 'DD-MM-YYYY HH24:MI')
        else null
    end as transaction_date_ts,

    case when completed_at ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$' 
        then to_timestamp(completed_at, 'DD-MM-YYYY HH24:MI')
        else null
    end as completed_at_ts,

    case when cancelled_at ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$' 
        then to_timestamp(cancelled_at, 'DD-MM-YYYY HH24:MI')
        else null
    end as cancelled_at_ts,

    case when created_at ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$' 
        then to_timestamp(created_at, 'DD-MM-YYYY HH24:MI')
        else null
    end as created_at_ts,

    -- dispute status normalization
    case
        when lower(trim(dispute_status)) in ('none','null','') then 'none'
        when lower(trim(dispute_status)) = 'raised' then 'raised'
        when lower(trim(dispute_status)) = 'resolved' then 'resolved'
        else lower(trim(dispute_status))
    end as dispute_status_norm

from {{ source('public','raw_p2p_transactions') }}

