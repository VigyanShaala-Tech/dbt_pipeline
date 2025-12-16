{{ config(
  materialized='table'
) }}

WITH submission_counts AS (
  SELECT
    student_id,
    resource_id,
    title,
    cohort_code,
    college_name,
    COUNT(*) FILTER (WHERE submission_status = 'under review') AS under_review_count,
    COUNT(*) FILTER (WHERE submission_status = 'reviewed') AS accepted_count,
    COUNT(*) FILTER (WHERE submission_status = 'rejected') AS rejected_count,
    MAX(submitted_at) FILTER (WHERE submission_status = 'under review') AS last_submission_date
  FROM intermediate.final_assignment
  GROUP BY student_id, resource_id, title, cohort_code, college_name
)

SELECT
  sc.student_id,
  sd.email AS email_id,
  sc.resource_id,
  sc.title,
  sc.cohort_code,
  sc.college_name,
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
INNER JOIN raw.student_details sd
  ON sc.student_id = sd.id