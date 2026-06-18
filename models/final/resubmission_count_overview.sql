{{ config(
  materialized='table'
) }}

WITH submission_counts AS (
  SELECT
    fa.student_id,
    fa.resource_id,
    fa.resource_title,
    fa.cohort_code,
    sd.final_college_name,
    COUNT(*) FILTER (WHERE fa.submission_status = 'under review') AS under_review_count,
    COUNT(*) FILTER (WHERE fa.submission_status = 'reviewed') AS accepted_count,
    COUNT(*) FILTER (WHERE fa.submission_status = 'rejected') AS rejected_count,
    MAX(fa.submitted_at) FILTER (WHERE fa.submission_status = 'under review') AS last_submission_date
  FROM {{ ref('final_assignment') }} fa
  LEFT JOIN {{ ref('student_demography') }} sd
      ON fa.student_id = sd.student_id
      AND fa.cohort_code = sd.cohort_code
  GROUP BY fa.student_id, fa.resource_id, fa.resource_title, fa.cohort_code, sd.final_college_name
)

SELECT
  sc.student_id,
  sd.email AS email_id,
  sc.resource_id,
  sc.resource_title,
  sc.cohort_code,
  sc.final_college_name,
  sc.under_review_count AS total_submissions,
  CASE WHEN sc.under_review_count > 1 THEN sc.under_review_count - 1 ELSE 0 END AS resubmissions_count,
  CASE 
    WHEN sc.under_review_count > 1 
    THEN ROUND(( (sc.under_review_count - 1)::numeric / sc.under_review_count ) * 100, 2)
    ELSE 0 
  END AS resubmission_rate,
  sc.accepted_count,
  ROUND((sc.accepted_count::numeric / NULLIF(sc.under_review_count, 0)) * 100, 2) AS acceptance_rate,
  sc.rejected_count,
  ROUND((sc.rejected_count::numeric / NULLIF(sc.under_review_count, 0)) * 100, 2) AS rejection_rate,
  sc.last_submission_date
FROM submission_counts sc
INNER JOIN {{ source('raw', 'student_details') }} sd
  ON sc.student_id = sd.id