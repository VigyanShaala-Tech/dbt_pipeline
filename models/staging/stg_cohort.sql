{{
  config(
    materialized='table',
    tags=['staging', 'cohort']
  )
}}


with osot_cohort as (

    select
        cohort_code::text as cohort_code,
        program_code::text as program_code,
        cohort_number::text as cohort_number,
        cohort_name::text as cohort_name,
        type::text as type,
        start_date,
        end_date,
        is_active,
        'OSOT'::text as source_system

    from {{ source('raw', 'cohort') }}

),


openedx_cohort as (

    select
        id::text as cohort_code,

        case
            when display_name ilike '%incubator%' then 'INC'
            when display_name ilike '%accelerator%' then 'ACC'
            else null
        end::text as program_code,

        null as cohort_number,
        display_name::text as cohort_name,

        cast(null as text) as type,

        start as start_date,
        "end" as end_date,

        case
            when current_date between start::date and "end"::date
                then true
            else false
        end as is_active,

        'Open edX'::text as source_system

    from {{ source('raw', 'openedx_course_overviews_courseoverview') }}

)

select * from osot_cohort
union all
select * from openedx_cohort