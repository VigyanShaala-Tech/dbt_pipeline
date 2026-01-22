{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['student_id', 'resource_id', 'submitted_at']
) }}

WITH raw_student_cohort_data AS (
    SELECT
        student_id,
        cohort_code
    FROM {{ source('raw', 'student_cohort') }}
),

student_details_data AS (
    SELECT id,email FROM raw.student_details
),

assignment_data AS (
    SELECT
        assignment_name AS name,
        "Email" AS email,
        student_name,
        submission_status,
        feedback_comments AS feedback,
        submitted_at,
        assignment_file
    FROM {{ source('old', 'assignment_monitoring_data') }}
),

resource_data AS (
    SELECT
        id AS resource_id,
        title
    FROM {{ source('raw', 'resource') }}
    WHERE category = 'Assignment'
),

student_assignment_data AS (
    SELECT
        sd.id::INT AS student_id,
        r.resource_id::INT AS resource_id,
        NULL::INT AS mentor_id,
        sc.cohort_code,
        a.submission_status::raw.submission_status_enum AS submission_status,
        CASE 
            WHEN a.submission_status = 'under review' THEN 30
            WHEN a.submission_status = 'reviewed' THEN 100
            WHEN a.submission_status = 'rejected' THEN 80
            ELSE 0
        END::DECIMAL AS marks_pct,
        a.feedback AS feedback_comments,
        NULLIF(TRIM(a.submitted_at), '')::timestamp AS submitted_at,
        a.assignment_file
    FROM assignment_data a
    INNER JOIN student_details_data sd
        ON a.email = sd.email
    INNER JOIN raw_student_cohort_data sc
        ON sd.id = sc.student_id
    INNER JOIN resource_data r
        ON a.name = r.title
    WHERE TRIM(a.submitted_at) IS NOT NULL
      AND TRIM(a.submitted_at) <> ''
      AND TRIM(a.submitted_at) <> 'NaN'
)

SELECT *
FROM student_assignment_data
WHERE cohort_code IN ('INC007','INC008','INC009')    --Change as per cohorts that needs to be updated, will reduce runtime.  