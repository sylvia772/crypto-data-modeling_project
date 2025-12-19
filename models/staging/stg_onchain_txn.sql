{{ config(materialized='view') }}

select
    -- keys
    trim(transaction_id) as transaction_id,
    trim(txn_hash) as txn_hash,
    trim(user_id) as user_id,

    -- normalize txn_type
    case
        when lower(trim(txn_type)) in ('deposit') then 'deposit'
        when lower(trim(txn_type)) in ('withdrawal') then 'withdrawal'
        else lower(trim(txn_type))
    end as txn_type,

    -- amounts
    -- strip symbols/letters, cast to numeric
    case
        when amount is null then null
        else regexp_replace(lower(trim(amount)), '[a-z$ ]', '', 'g')::numeric
    end as amount_num,

    case
        when usd_value is null then null
        else regexp_replace(lower(trim(usd_value)), '[a-z$ ]', '', 'g')::numeric
    end as usd_value_num,

    case
        when fee is null then null
        else regexp_replace(lower(trim(fee)), '[a-z$ ]', '', 'g')::numeric
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
    nullif(block_number,'')::bigint as block_number,

    -- confirmations
    nullif(confirmations,'')::int as confirmations,

    -- status normalization
    case
        when lower(trim(status)) in ('completed','success','1','true') then 'completed'
        when lower(trim(status)) in ('pending') then 'pending'
        when lower(trim(status)) in ('failed','error','0','false') then 'failed'
        else lower(trim(status))
    end as status_norm,

    -- timestamps
    created_date::timestamp as created_at_ts,
    completed_date::timestamp as completed_date_ts,

    -- network normalization
    case
        when lower(trim(network)) in ('mainnet','bitcoin') then 'mainnet'
        when lower(trim(network)) in ('testnet') then 'testnet'
        else lower(trim(network))
    end as network_norm,

    notes

from {{ source('public','raw_onchain_txn') }}
