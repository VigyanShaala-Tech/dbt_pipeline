{{
    config(
        materialized='table',
        tags=['final', 'resubmission_overview']
    )
}}

with submission_counts as (

    select
        fa.student_id,
        fa.resource_id,
        fa.cohort_code,
        sd.final_college_name,

        count(*) filter (
            where lower(fa.submission_status::text) = 'under review'
        ) as under_review_count,

        count(*) filter (
            where lower(fa.submission_status::text) = 'accepted'
        ) as accepted_count,

        count(*) filter (
            where lower(fa.submission_status::text) = 'reattempt'
        ) as rejected_count,

        max(fa.submitted_at) filter (
            where lower(fa.submission_status::text) = 'under review'
        ) as last_submission_date

    from {{ ref('int_final_assignment') }} fa

    left join {{ ref('int_student_demography') }} sd
        on fa.student_id::text = sd.student_id::text
       and fa.cohort_code::text = sd.cohort_code::text

    group by
        fa.student_id,
        fa.resource_id,
        fa.cohort_code,
        sd.final_college_name

)

select

    sc.student_id,
    sd.email as email_id,

    sc.resource_id,
    sc.cohort_code,
    sc.final_college_name,

    sc.under_review_count as total_submissions,

    case
        when sc.under_review_count > 1
            then sc.under_review_count - 1
        else 0
    end as resubmissions_count,

    case
        when sc.under_review_count > 1
            then round(
                (
                    (sc.under_review_count - 1)::numeric
                    / sc.under_review_count
                ) * 100,
                2
            )
        else 0
    end as resubmission_rate,

    sc.accepted_count,

    round(
        (
            sc.accepted_count::numeric
            / nullif(sc.under_review_count, 0)
        ) * 100,
        2
    ) as acceptance_rate,

    sc.rejected_count,

    round(
        (
            sc.rejected_count::numeric
            / nullif(sc.under_review_count, 0)
        ) * 100,
        2
    ) as rejection_rate,

    sc.last_submission_date

from submission_counts sc

inner join {{ ref('int_student_demography') }} sd
    on sc.student_id::text = sd.student_id::text