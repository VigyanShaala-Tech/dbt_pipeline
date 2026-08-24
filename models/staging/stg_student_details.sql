{{
  config(
    materialized='table',
    tags=['staging', 'student_details']
  )
}}

-- Historical OSOT rows: untouched, real columns confirmed from the repo.
-- location_id stays as-is; there is no Open edX equivalent (Conflict A).
with osot_rows as (

    select
        cast(id as text)                         as student_id,
        email,
        phone,
        first_name,
        last_name,
        cast(gender as text) as gender,
        date_of_birth,
        caste,
        annual_family_income_inr,
        location_id,
        cast(null as text)          as country_raw,
        cast(null as text)          as state_union_territory_raw,
        cast(null as text)          as district_raw,
        cast(null as text)          as city_category_raw,
        inserted_at,
        updated_at,
        'OSOT'                      as source_system,
        'historical'                as matched_by,
        cast(null as varchar)       as matched_value

    from {{ source('raw', 'student_details') }}

),

-- Open edX rows: auth_user + auth_userprofile + user_metadata, no OSOT overwrite.
-- Geography lands in *_raw columns (Conflict A) — location_id stays NULL rather
-- than guessing a location_mapping row.
openedx_base as (

    select
        au.id as openedx_user_id,
        au.email,
        au.first_name,
        au.last_name,
        cast(up.gender as text) as gender,
        up.phone_number as phone,

        (
            NULLIF(um.dynamic_fields_data, '')::jsonb
            -> '04_additional_information'
            ->> '17_date_of_birth'
        )::date as date_of_birth,

        NULLIF(um.dynamic_fields_data, '')::jsonb
            -> '02_hometown_information'
            ->> '10_caste_category' as caste,

        NULLIF(um.dynamic_fields_data, '')::jsonb
            -> '02_hometown_information'
            ->> '11_annual_household_income'
            as annual_family_income_inr,

        cast(null as integer) as location_id,

        NULLIF(um.dynamic_fields_data, '')::jsonb
            -> '02_hometown_information'
            ->> '06_hometown_country'
            as country_raw,

        NULLIF(um.dynamic_fields_data, '')::jsonb
            -> '02_hometown_information'
            ->> '07_hometown_state'
            as state_union_territory_raw,

        NULLIF(um.dynamic_fields_data, '')::jsonb
            -> '02_hometown_information'
            ->> '08_hometown_district'
            as district_raw,

        NULLIF(um.dynamic_fields_data, '')::jsonb
            -> '02_hometown_information'
            ->> '09_hometown_city_category'
            as city_category_raw,

        um.created as inserted_at,
        um.modified as updated_at

    from {{ source('raw', 'openedx_auth_user') }} au

    left join {{ source('raw', 'openedx_auth_userprofile') }} up
        on up.user_id = au.id

    left join {{ source('raw', 'openedx_user_metadata_app_usermetadata') }} um
        on um.user_id = au.id

),

openedx_rows as (

    select
        cast(x.student_id as text) as student_id,
        b.email,
        b.phone,
        b.first_name,
        b.last_name,
        b.gender,
        b.date_of_birth,
        b.caste,
        b.annual_family_income_inr,
        b.location_id,
        b.country_raw,
        b.state_union_territory_raw,
        b.district_raw,
        b.city_category_raw,
        b.inserted_at,
        b.updated_at,
        'Open edX'      as source_system,
        x.matched_by,
        x.matched_value

    from openedx_base b
    inner join {{ ref('stg_student_id_xref') }} x
        on x.openedx_user_id = b.openedx_user_id

)

select * from osot_rows
union all
select * from openedx_rows