
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
openedx_rows AS (

    SELECT
        x.student_id::text AS student_id,

        (ARRAY_AGG(
            csm.module_id::text
            ORDER BY csm.created DESC NULLS LAST
        ))[1] AS resource_id,

        csm.course_id::text AS cohort_code,
        TRIM(ccb.subsection_name) AS quiz_name,

        SUM(csm.grade::numeric) AS marks,
        15::numeric AS max_marks,

        MAX(
            (csm.state::jsonb ->> 'attempts')::integer
        ) AS reattempts,

        MAX(csm.created) AS attempted_at,

        'Open edX'::text AS source_system

    FROM {{ source('raw', 'Openedx_courseware_studentmodule') }} csm

    INNER JOIN {{ ref('stg_student_id_xref') }} x
        ON x.openedx_user_id = csm.student_id

    INNER JOIN {{ source('raw', 'Openedx_course_content_catalog_coursecontentblock') }} ccb
        ON TRIM(csm.module_id::text) = TRIM(ccb.usage_key::text)

    WHERE csm.module_type = 'problem'

    GROUP BY
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