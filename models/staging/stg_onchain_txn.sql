{{ config(materialized='view') }}

select
    -- keys
    trim(transaction_id) as transaction_id,
    trim(txn_hash) as txn_hash,
    trim(user_id) as user_id,

    -- normalize txn_type
    case
        when lower(trim(txn_type)) = 'deposit' then 'deposit'
        when lower(trim(txn_type)) = 'withdrawal' then 'withdrawal'
        else lower(trim(txn_type))
    end as txn_type,

    -- amounts (remove letters, symbols, commas)
    case
        when amount is null then null
        else regexp_replace(trim(amount), '[^0-9.]', '', 'g')::numeric
    end as amount_num,

    case
        when usd_value is null then null
        else regexp_replace(trim(usd_value), '[^0-9.]', '', 'g')::numeric
    end as usd_value_num,

    case
        when fee is null then null
        else regexp_replace(trim(fee), '[^0-9.]', '', 'g')::numeric
    end as fee_num,

    -- currency normalization
    case
        when lower(trim(currency)) in ('btc','bitcoin') then 'BTC'
        when lower(trim(currency)) in ('eth','ethereum') then 'ETH'
        else upper(trim(currency))
    end as currency_norm,

    -- addresses
    trim(from_address) as from_address,
    trim(to_address) as to_address,

    -- block number
    nullif(block_number, '')::bigint as block_number,

    -- confirmations
    nullif(confirmations, '')::int as confirmations,

    -- status normalization
    case
        when lower(trim(status)) in ('completed','success','1','true') then 'completed'
        when lower(trim(status)) = 'pending' then 'pending'
        when lower(trim(status)) in ('failed','error','0','false') then 'failed'
        else lower(trim(status))
    end as status_norm,

    -- created timestamp (handles both formats safely)
    case
        when created_date ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
            then to_timestamp(created_date, 'YYYY-MM-DD HH24:MI:SS')
        when created_date ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
            then to_timestamp(created_date, 'DD/MM/YYYY HH24:MI:SS')
        else null
    end as created_at_ts,

    -- completed timestamp (handles both formats safely)
    case
        when completed_date ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
            then to_timestamp(completed_date, 'YYYY-MM-DD HH24:MI:SS')
        when completed_date ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
            then to_timestamp(completed_date, 'DD/MM/YYYY HH24:MI:SS')
        else null
    end as completed_date_ts,

    -- network normalization
    case
        when lower(trim(network)) in ('mainnet','bitcoin') then 'mainnet'
        when lower(trim(network)) = 'testnet' then 'testnet'
        else lower(trim(network))
    end as network_norm,

    notes

from {{ source('public','raw_onchain_txn') }}
