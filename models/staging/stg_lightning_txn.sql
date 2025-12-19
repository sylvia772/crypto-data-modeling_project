{{ config(materialized='view') }}

select
    --  Primary identifiers
    trim(lightning_id) as lightning_id,          -- Lightning transaction ID
    trim(payment_hash) as payment_hash,          -- Payment hash from Lightning network
    trim(user_id) as user_id,                    -- User who initiated the transaction

    --  Normalize transaction direction
    case
        when lower(trim(txn_direction)) in ('inbound','in') then 'inbound'
        when lower(trim(txn_direction)) in ('outbound','out') then 'outbound'
        else lower(trim(txn_direction))            -- fallback
    end as txn_direction_norm,

    -- Amounts (clean commas, cast to numeric types)
    regexp_replace(trim(amount_sats), ',', '', 'g')::bigint as amount_sats_int,   -- satoshis
    regexp_replace(trim(amount_btc), ',', '', 'g')::numeric as amount_btc_num,    -- BTC
    regexp_replace(trim(fee_sats), ',', '', 'g')::bigint as fee_sats_int,         -- fee in satoshis

    -- Normalize status values
    case
        when lower(trim(status)) in ('settled','1','true','success') then 'settled'
        when lower(trim(status)) = 'pending' then 'pending'
        when lower(trim(status)) in ('failed','0','false','error') then 'failed'
        else lower(trim(status))
    end as status_norm,

    -- Invoice and routing info
    trim(payment_request) as payment_request,
    trim(destination) as destination,
    route_hints,
    trim(preimage) as preimage,
    memo,

    --  Handle timestamps safely
    -- Detect if value is epoch (digits only), DD-MM-YYYY, or ISO string
    case
      when timestamp ~ '^\d+$' then to_timestamp(timestamp::bigint)                 -- epoch seconds
      when timestamp ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
           then to_timestamp(timestamp, 'DD-MM-YYYY HH24:MI')                       -- day-first format
      else cast(timestamp as timestamp)                                             -- ISO fallback
    end as txn_timestamp,

    case
      when settled_at ~ '^\d+$' then to_timestamp(settled_at::bigint)
      when settled_at ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
           then to_timestamp(settled_at, 'DD-MM-YYYY HH24:MI')
      else cast(settled_at as timestamp)
    end as settled_at_ts,

    case
      when expiry ~ '^\d+$' then to_timestamp(expiry::bigint)
      when expiry ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
           then to_timestamp(expiry, 'DD-MM-YYYY HH24:MI')
      else cast(expiry as timestamp)
    end as expiry_ts,

    case
      when created ~ '^\d+$' then to_timestamp(created::bigint)
      when created ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
           then to_timestamp(created, 'DD-MM-YYYY HH24:MI')
      else cast(created as timestamp)
    end as created_ts,

    -- Boolean flag for keysend
    case
        when lower(trim(is_keysend)) in ('true','1','yes') then true
        when lower(trim(is_keysend)) in ('false','0','no') then false
        else null
    end as is_keysend_bool

from {{ source('public','raw_lightning_txn') }}

