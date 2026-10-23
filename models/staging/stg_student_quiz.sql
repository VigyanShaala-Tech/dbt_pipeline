
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
        TRIM(ccb.subsection_name) AS quiz_name,
        sum(csm.grade::numeric) as marks,

        15::numeric as max_marks,

        max(
            (csm.state::jsonb ->> 'attempts')::integer
        ) as reattempts,

        max(csm.created) as attempted_at,

        'Open edX'::text as source_system

    from {{ source('raw', 'Openedx_courseware_studentmodule') }} csm

    inner join {{ ref('stg_student_id_xref') }} x
        on x.openedx_user_id = csm.student_id

    INNER JOIN {{ source('raw', 'Openedx_course_content_catalog_coursecontentblock') }} ccb
        ON TRIM(csm.module_id::text) = TRIM(ccb.usage_key::text)

    where csm.module_type = 'problem'

    group by
        x.student_id,
        csm.course_id,
        TRIM(ccb.subsection_name)
)

SELECT
    student_id,
    resource_id,
    cohort_code,
    marks,
    max_marks,
    reattempts,
    attempted_at,
    source_system
FROM osot_rows

UNION ALL

SELECT
    student_id,
    resource_id,
    cohort_code,
    marks,
    max_marks,
    reattempts,
    attempted_at,
    source_system
FROM openedx_rows