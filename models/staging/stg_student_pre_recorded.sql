{{
  config(
    materialized='table',
    tags=['staging', 'student_pre_recorded']
  )
}}

-- OSOT pre-recorded watch data
with osot_rows as (

    select
        student_id::text as student_id,
        resource_id::text as resource_id,
        cohort_code::text as cohort_code,
        watchtime_in_sec::integer as watchtime_in_sec,
        watched_at as watched_at,
        'OSOT'::text as source_system

    from {{ source('raw', 'student_pre_recorded') }}

),

-- Open edX video watch data
openedx_rows as (

    select
        x.student_id::text as student_id,
        csm.module_id::text as resource_id,
        csm.course_id::text as cohort_code,

        extract(
            epoch from (
                csm.state::jsonb ->> 'saved_video_position'
            )::interval
        )::integer as watchtime_in_sec,

        csm.created as watched_at,

        'Open edX'::text as source_system

    from {{ source('raw', 'openedx_courseware_studentmodule') }} csm

    inner join {{ ref('stg_student_id_xref') }} x
        on x.openedx_user_id = csm.student_id

    where csm.module_type = 'video'

)

select * from osot_rows
union all
select * from openedx_rows