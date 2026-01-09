{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['student_id', 'resource_id']
) }}

WITH raw_student_cohort_data AS (
    SELECT student_id, cohort_code
    FROM raw.student_cohort
),
quiz_data AS (
    SELECT
        "user_id" AS email,
        "data_fields" AS quiz_name,
        "value" AS obtained_marks
    FROM old.incubator_quiz_monitoring
),
                          
student_details_data AS (
    SELECT id,email FROM raw.student_details
),
resource_data AS (
    SELECT id AS resource_id, title
    FROM raw.resource
    WHERE category = 'Quiz'
),
student_quiz_data AS (
    SELECT
        sd.id::INT AS student_id,
        r.resource_id::INT AS resource_id,
        sc.cohort_code AS cohort_code,
        100::INT AS max_marks,
        q.obtained_marks::INT AS marks,
        NULL::INT AS reattempts,
        NULL::TIMESTAMP AS attempted_at
    FROM quiz_data q
    INNER JOIN student_details_data sd ON q.email = sd.email                    
    INNER JOIN raw_student_cohort_data sc ON sd.id = sc.student_id
    INNER JOIN resource_data r ON q.quiz_name = r.title
)

SELECT *
FROM student_quiz_data
WHERE cohort_code IN ('INC007')  --Change as per cohorts that needs to be updated, will reduce runtime. 