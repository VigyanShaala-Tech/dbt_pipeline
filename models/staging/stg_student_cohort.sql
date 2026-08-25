{{
  config(
    materialized='table',
    tags=['staging', 'student_cohort']
  )
}}


with osot_rows as (

    select
        student_code::text as student_code,
        student_id::text as student_id,
        cohort_code::text as cohort_code,
        is_leader::boolean as is_leader,
        cohort_enroll_date,
        'OSOT'::text as source_system

    from {{ source('raw', 'student_cohort') }}

),

openedx_rows as (

    select
        (
            right(extract(year from sc.start_date)::text, 2)
            || sc.program_code::text
            || lpad(sc.cohort_number::text, 3, '0')
            || lpad(x.openedx_user_id::text, 7, '0')
        )::text as student_code,

        x.student_id::text as student_id,

        (
            sc.program_code::text
            || lpad(sc.cohort_number::text, 3, '0')
        )::text as cohort_code,

        false::boolean as is_leader,

        sce.created as cohort_enroll_date,

        'Open edX'::text as source_system

    from {{ source('raw', 'openedx_student_courseenrollment') }} sce

    inner join {{ ref('stg_student_id_xref') }} x
        on x.openedx_user_id = sce.user_id

    inner join {{ source('raw', 'openedx_course_overviews_courseoverview') }} co
        on co.id = sce.course_id

    inner join {{ ref('stg_cohort') }} sc
        on sc.cohort_name = co.display_name
       and sc.source_system = 'Open edX'

)


select * from osot_rows
union all
select * from openedx_rows