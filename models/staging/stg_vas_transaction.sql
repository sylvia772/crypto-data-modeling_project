{{ config(materialized='view') }}

select
    --  Primary identifiers
    trim(vas_id) as vas_id,                    -- VAS transaction ID
    trim(user_id) as user_id,                  -- User making purchase

    --  Normalize service type
    case
        when lower(trim(service_type)) in ('airtime') then 'airtime'
        when lower(trim(service_type)) in ('data') then 'data'
        when lower(trim(service_type)) in ('bills') then 'bills'
        else lower(trim(service_type))            -- fallback
    end as service_type_norm,

    -- Normalize provider
    case
        when lower(trim(provider)) = 'mtn' then 'MTN'
        when lower(trim(provider)) = 'airtel' then 'Airtel'
        when lower(trim(provider)) = 'dstv' then 'DSTV'
        else initcap(trim(provider))              -- fallback: capitalize first letter
    end as provider_norm,

    -- Amounts
    case
        when amount is null then null
        else regexp_replace(lower(trim(amount)), '[a-z$ ]', '', 'g')::numeric
    end as amount_num,

    --  Currency normalization
    case
        when lower(trim(currency)) = 'ngn' then 'NGN'
        when lower(trim(currency)) = 'usd' then 'USD'
        else upper(trim(currency))
    end as currency_norm,

    --  Product and recipient
    trim(product_code) as product_code,
    trim(recipient_number) as recipient_number,

    --  Normalize status
    case
        when lower(trim(status)) in ('success','1','true') then 'success'
        when lower(trim(status)) = 'pending' then 'pending'
        when lower(trim(status)) in ('failed','0','false','error') then 'failed'
        else lower(trim(status))
    end as status_norm,

    --  Handle transaction_date safely
    case
      when transaction_date ~ '^\d+$' then to_timestamp(transaction_date::bigint)   -- epoch seconds
      when transaction_date ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
           then to_timestamp(transaction_date, 'DD-MM-YYYY HH24:MI')                -- day-first format
      else cast(transaction_date as timestamp)                                     -- ISO fallback
    end as transaction_date_ts,

    --  Handle completed_date safely
    case
      when completed_date ~ '^\d+$' then to_timestamp(completed_date::bigint)
      when completed_date ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
           then to_timestamp(completed_date, 'DD-MM-YYYY HH24:MI')
      else cast(completed_date as timestamp)
    end as completed_date_ts,

    --  Commission
    case
        when commission is null then null
        else regexp_replace(lower(trim(commission)), '[a-z$ ]', '', 'g')::numeric
    end as commission_num,

    -- External reference
    trim(external_reference) as external_reference,

    --  Retry count
    nullif(retry_count,'')::int as retry_count_int,

    -- Error and API response
    error_message,
    api_response,

    --  Handle created_at safely
    case
      when created_at ~ '^\d+$' then to_timestamp(created_at::bigint)
      when created_at ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
           then to_timestamp(created_at, 'DD-MM-YYYY HH24:MI')
      else cast(created_at as timestamp)
    end as created_at_ts

from {{ source('public','raw_vas_transactions') }}
