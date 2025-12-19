{{ config(materialized='view') }}

select
  -- keys and identity
  trim(user_id) as user_id,
  lower(trim(UserName)) as username,

  -- contacts
  lower(trim(email_address)) as email,
  trim(phone) as phone_raw,

  -- timestamps (basic cast only)
  created_at::timestamp as created_at_ts,
  last_login::timestamp as last_login_ts,
  verification_date::timestamp as verification_date_ts,
  updated_at::timestamp as updated_at_ts,

  -- date of birth: basic cast
  date_of_birth::date as date_of_birth,

  -- country: basic normalization
  case
    when country is null or trim(country) = '' then null
    else trim(country)
  end as country,

  -- kyc status: light normalization
  case
    when lower(trim(kyc_status)) in ('verified','true','1','yes') then 'verified'
    when lower(trim(kyc_status)) in ('pending','in_progress') then 'pending'
    when lower(trim(kyc_status)) in ('rejected','failed','declined') then 'rejected'
    else lower(trim(kyc_status))
  end as kyc_status,

  -- account type: light normalization
  case
    when lower(trim(account_type)) in ('individual','personal') then 'individual'
    when lower(trim(account_type)) in ('business','merchant') then 'business'
    else lower(trim(account_type))
  end as account_type,

  -- referral fields
  trim(referral_code) as referral_code,
  trim(referred_by) as referred_by,

  -- wallet
  trim(wallet_address) as wallet_address,

  -- registration source: light normalization
  case
    when lower(trim(registration_source)) in ('ios','iphone') then 'ios'
    when lower(trim(registration_source)) = 'android' then 'android'
    when lower(trim(registration_source)) in ('web','website') then 'web'
    when lower(trim(registration_source)) in ('mobile_app','app') then 'mobile_app'
    else lower(trim(registration_source))
  end as registration_source,

  -- status: light normalization
  case
    when lower(trim(status)) in ('active','1','true') then 'active'
    when lower(trim(status)) in ('inactive','0','false','suspended') then 'inactive'
    else lower(trim(status))
  end as status,

  -- numerics
  nullif(total_transactions,'')::int as total_transactions,
  -- very light cleaning: strip commas and $; ignore currency codes for now
  replace(replace(account_balance, ',', ''), '$', '')::numeric as account_balance,

  -- tier: light normalization
  case
    when lower(trim(tier)) in ('tier1','tier 1','1','basic') then 'tier_1'
    when lower(trim(tier)) in ('tier2','tier 2','2') then 'tier_2'
    when lower(trim(tier)) in ('tier3','tier 3','3') then 'tier_3'
    else lower(trim(tier))
  end as tier,

  -- notes
  notes

from {{ source('public', 'raw_users') }}