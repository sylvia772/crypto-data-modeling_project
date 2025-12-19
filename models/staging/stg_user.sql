{{ config(materialized='view') }}

select
    -- Primary identifiers
    trim(user_id) as user_id,
    trim(UserName) as username,
    trim(email_address) as email,
    
    -- Clean phone numbers (remove spaces, parentheses, etc.)
    regexp_replace(trim(phone), '[^0-9+]', '', 'g') as phone_clean,
    
    -- Normalize country
    case
        when lower(trim(country)) in ('nigeria', 'ng') then 'Nigeria'
        else initcap(trim(country))
    end as country_norm,
    
    -- Normalize KYC status (verification status)
    case
        when lower(trim(kyc_status)) in ('verified', '1', 'true') then 'verified'
        when lower(trim(kyc_status)) = 'pending' then 'pending'
        when lower(trim(kyc_status)) in ('unverified', 'not verified', '0', 'false') then 'unverified'
        else lower(trim(kyc_status))
    end as kyc_status_norm,
    
    -- Normalize account type
    case
        when lower(trim(account_type)) = 'individual' then 'individual'
        when lower(trim(account_type)) = 'business' then 'business'
        else lower(trim(account_type))
    end as account_type_norm,
    
    -- Normalize account status
    case
        when lower(trim(status)) in ('active', '1', 'true') then 'active'
        when lower(trim(status)) in ('inactive', '0', 'false') then 'inactive'
        when lower(trim(status)) = 'suspended' then 'suspended'
        else lower(trim(status))
    end as status_norm,
    
    -- Normalize tier
    case
        when lower(trim(tier)) in ('tier1', 'tier 1', '1', 'basic') then 'tier1'
        when lower(trim(tier)) in ('tier2', 'tier 2', '2') then 'tier2'
        when lower(trim(tier)) in ('tier3', 'tier 3', '3') then 'tier3'
        else lower(trim(tier))
    end as tier_norm,
    
    -- Identifiers
    trim(referral_code) as referral_code,
    trim(referred_by) as referred_by,
    trim(wallet_address) as wallet_address,
    trim(registration_source) as registration_source,
    
    -- Normalize date of birth (handle age vs date)
    case
        when date_of_birth ~ '^\d{1,2}$' then null  -- Skip if it's just an age number
        when date_of_birth ~ '^\d{2}/\d{2}/\d{4}$' 
            then to_date(date_of_birth, 'DD/MM/YYYY')
        when date_of_birth ~ '^\d{4}-\d{2}-\d{2}$'
            then cast(date_of_birth as date)
        else null
    end as date_of_birth,
    
    -- Calculate age if DOB is available, otherwise extract from raw field
    case
        when date_of_birth ~ '^\d{1,2}$' then date_of_birth::int  -- Direct age
        when date_of_birth ~ '^\d{2}/\d{2}/\d{4}$' 
            then extract(year from age(to_date(date_of_birth, 'DD/MM/YYYY')))::int
        when date_of_birth ~ '^\d{4}-\d{2}-\d{2}$'
            then extract(year from age(cast(date_of_birth as date)))::int
        else null
    end as age,
    
    -- Handle timestamps (epoch, DD/MM/YYYY HH24:MI:SS, or ISO)
    case
        when created_at ~ '^\d+$' then to_timestamp(created_at::bigint)
        when created_at ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
            then to_timestamp(created_at, 'DD/MM/YYYY HH24:MI:SS')
        when created_at ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
            then cast(created_at as timestamp)
        when created_at ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$'
            then cast(created_at as timestamp)
        else null
    end as created_at_ts,
    
    case
        when verification_date ~ '^\d+$' then to_timestamp(verification_date::bigint)
        when verification_date ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
            then cast(verification_date as timestamp)
        when verification_date ~ '^\d{4}-\d{2}-\d{2}$'
            then cast(verification_date as timestamp)
        else null
    end as verification_date_ts,
    
    case
        when last_login ~ '^\d+$' then to_timestamp(last_login::bigint)
        when last_login ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
            then to_timestamp(last_login, 'DD/MM/YYYY HH24:MI:SS')
        when last_login ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
            then cast(last_login as timestamp)
        when last_login ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$'
            then cast(last_login as timestamp)
        else null
    end as last_login_ts,
    
    case
        when updated_at ~ '^\d+$' then to_timestamp(updated_at::bigint)
        when updated_at ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
            then to_timestamp(updated_at, 'DD/MM/YYYY HH24:MI:SS')
        when updated_at ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
            then cast(updated_at as timestamp)
        when updated_at ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$'
            then cast(updated_at as timestamp)
        else null
    end as updated_at_ts,
    
    -- Transaction count (clean numbers)
    case
        when total_transactions ~ '^\d+$' then total_transactions::int
        else null
    end as total_transactions_int,
    
    -- Clean balance (remove currency symbols, commas, text)
    case
        when account_balance is not null then
            regexp_replace(
                regexp_replace(trim(account_balance), '[^0-9.]', '', 'g'),
                '\.(?=.*\.)', '', 'g'  -- Remove all but last decimal point
            )::numeric
        else null
    end as account_balance_num,
    
    -- Notes/metadata
    trim(notes) as notes

from {{ source('public', 'raw_users') }}
where trim(user_id) is not null  -- Filter out any rows without user_id
