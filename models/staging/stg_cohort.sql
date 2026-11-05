
{{
  config(
    materialized='table',
    tags=['staging', 'cohort']
  )
}}

WITH osot_cohort AS (

    SELECT
        cohort_code::text AS cohort_code,
        program_code::text AS program_code,
        cohort_number::text AS cohort_number,
        cohort_name::text AS cohort_name,
        type::text AS type,
        start_date,
        end_date,

        CASE
            WHEN start_date IS NOT NULL
                 AND end_date IS NOT NULL
                 AND (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Kolkata')::date
                     BETWEEN start_date::date AND end_date::date
            THEN TRUE
            ELSE FALSE
        END AS is_active,

        'OSOT'::text AS source_system

    FROM {{ source('raw', 'cohort') }}

),

openedx_cohort AS (

    SELECT
        id::text AS cohort_code,

        CASE
            WHEN display_name ILIKE '%incubator%' THEN 'INC'
            WHEN display_name ILIKE '%accelerator%' THEN 'ACC'
            ELSE NULL
        END::text AS program_code,

        NULL::text AS cohort_number,
        display_name::text AS cohort_name,

        NULL::text AS type,

        start AS start_date,
        "end" AS end_date,

        CASE
            WHEN start IS NOT NULL
                 AND "end" IS NOT NULL
                 AND (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Kolkata')::date
                     BETWEEN start::date AND "end"::date
            THEN TRUE
            ELSE FALSE
        END AS is_active,

        'Open edX'::text AS source_system

    FROM {{ source('raw', 'Openedx_course_overviews_courseoverview') }}

)

SELECT * FROM osot_cohort
UNION ALL
SELECT * FROM openedx_cohort