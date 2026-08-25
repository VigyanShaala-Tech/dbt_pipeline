{{
  config(
    materialized='table',
    tags=['staging', 'live_session']
  )
}}


with osot_session as (

    select
        id::text as live_session_id,

        cast(null as text) as openedx_session_id,

        cohort_code::text as cohort_code,
        session_name::text as session_name,
        type::text as type,
        code::text as code,
        duration_in_sec,
        conducted_on,

        'OSOT'::text as source_system

    from {{ source('raw', 'live_session') }}

),


openedx_session as (

    select
        concat('OPENEDX_', internal_id::text) as live_session_id,

        internal_id::text as openedx_session_id,

        course_id::text as cohort_code,
        topic::text as session_name,

        case
            when topic ilike '%speak up kalpana session%' then 'Speak Up Kalpana Session'
            when topic ilike '%workshop%' then 'Workshop'
            when topic ilike '%masterclass%' then 'Masterclass'
            else null
        end::text as type,

        case
            when topic ilike '%speak up kalpana session%' then 'SUK'
            when topic ilike '%workshop%' then 'WS'
            when topic ilike '%masterclass%' then 'MC'
            else null
        end::text as code,

        (duration_minutes * 60)::integer as duration_in_sec,
        schedule_time as conducted_on,

        'Open edX'::text as source_system

    from {{ source('raw', 'openedx_zoom_integration_meetings') }}

)

select * from osot_session
union all
select * from openedx_session