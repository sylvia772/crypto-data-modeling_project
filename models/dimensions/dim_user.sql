{{ config(materialized='table') }}

select
    user_id,                         -- primary key
    username,                        -- normalized username
    email,                           -- lowercase email
    phone_clean as phone,        -- cleaned phone number
    date_of_birth,                   -- parsed DOB
    country_norm as country,         -- standardized country
    kyc_status_norm as kyc_status,   -- normalized KYC status
    account_type_norm as account_type,
    status_norm as account_status,
    tier_norm as tier,
    registration_source as registration_source,
    created_at_ts as created_at,
    last_login_ts as last_login,
    verification_date_ts as verification_date,
    updated_at_ts as updated_at
from {{ ref('stg_user') }}
