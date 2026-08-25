{{
  config(
    materialized='table',
    tags=['staging', 'student_quiz']
  )
}}

-- OSOT quiz records
with osot_rows as (

    select
        student_id::text as student_id,
        resource_id::text as resource_id,
        cohort_code::text as cohort_code,
        marks::numeric as marks,
        max_marks::numeric as max_marks,
        reattempts::integer as reattempts,
        attempted_at,
        'OSOT'::text as source_system

    from {{ source('raw', 'student_quiz') }}

),

-- Open edX quiz records
openedx_rows as (

    select
        x.student_id::text as student_id,

        csm.module_id::text as resource_id,

        csm.course_id::text as cohort_code,

        csm.max_grade::numeric as marks,

        20::numeric as max_marks,

        (csm.state::jsonb ->> 'attempts')::integer
            as reattempts,

        csm.created as attempted_at,

        'Open edX'::text as source_system

    from {{ source('raw', 'openedx_courseware_studentmodule') }} csm

    inner join {{ ref('stg_student_id_xref') }} x
        on x.openedx_user_id = csm.student_id

    where csm.module_type = 'problem'

)

select * from osot_rows
union all
select * from openedx_rows