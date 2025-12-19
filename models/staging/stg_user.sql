{{ config(materialized='view') }}

select
  /* -----------------------------
     USER IDENTITY
  ----------------------------- */
  trim(user_id) as user_id,
  lower(trim(username)) as username,

  /* -----------------------------
     CONTACT DETAILS
  ----------------------------- */
  lower(trim(email_address)) as email,
  trim(phone) as phone_raw,

  /* -----------------------------
     CREATED AT
     Handles:
     - YYYY-MM-DD HH:MI:SS+TZ
     - DD/MM/YYYY HH:MI:SS
     - empty strings
  ----------------------------- */
  case
    when created_at is null or trim(created_at) = '' then null

    -- ISO timestamp with timezone
    when created_at ~ '^\d{4}-\d{2}-\d{2} .*[\+\-]\d{2}$'
      then created_at::timestamptz

    -- DD/MM/YYYY timestamp
    when created_at ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
      then to_timestamp(created_at, 'DD/MM/YYYY HH24:MI:SS')

    else null
  end as created_at_ts,

  /* -----------------------------
     DATE OF BIRTH
     Handles ISO and DD/MM/YYYY
  ----------------------------- */
  case
    when date_of_birth is null or trim(date_of_birth) = '' then null
    when date_of_birth ~ '^\d{4}-\d{2}-\d{2}$'
      then date_of_birth::date
    when date_of_birth ~ '^\d{2}/\d{2}/\d{4}$'
      then to_date(date_of_birth, 'DD/MM/YYYY')
    else null
  end as date_of_birth,

  /* -----------------------------
     COUNTRY
  ----------------------------- */
  nullif(trim(country), '') as country,

  /* -----------------------------
     KYC STATUS
  ----------------------------- */
  case
    when lower(trim(kyc_status)) in ('verified','true','1','yes') then 'verified'
    when lower(trim(kyc_status)) in ('pending','in_progress') then 'pending'
    when lower(trim(kyc_status)) in ('rejected','failed','declined') then 'rejected'
    else lower(trim(kyc_status))
  end as kyc_status

from {{ source('public', 'raw_users') }}
