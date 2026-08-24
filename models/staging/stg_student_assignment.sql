{{
  config(
    materialized='table',
    tags=['staging', 'student_assignment']
  )
}}

with osot_rows as (

    select
        sa.id::text as id,
        sa.student_id::text as student_id,
        sa.resource_id::text as resource_id,
        sa.mentor_id::text as mentor_id,
        sa.cohort_code::text as cohort_code,
        sa.submission_status::text as submission_status,

        -- OSOT source only contains marks_pct
        null::numeric as marks,
        null::numeric as max_marks,
        sa.marks_pct::numeric as marks_pct,

        sa.feedback_comments::text as feedback_comments,
        sa.submitted_at as submitted_at,
        sa.assignment_file::text as assignment_file,

        'OSOT'::text as source_system

    from {{ source('raw', 'student_assignment') }} sa

),


-- Calculate marks and maximum marks from TAS rubric JSON.
-- Each rubric criterion is out of 10 marks.
openedx_feedback as (

    select
        ifb.submission_id,

        max(ifb.comment)::text as feedback_comments,

        coalesce(
            sum(
                case
                    when criterion.value ->> 'marks' is not null
                        then (criterion.value ->> 'marks')::numeric
                    else 0
                end
            ),
            0
        )::numeric as marks,

        (
            count(criterion.value) * 10
        )::numeric as max_marks

    from {{ source('raw', 'openedx_tas_app_instructorfeedback') }} ifb

    left join lateral
        jsonb_array_elements(
            coalesce(
                ifb.rubrics::jsonb,
                '[]'::jsonb
            )
        ) as criterion(value)
        on true

    group by ifb.submission_id

),


openedx_rows as (

    select
        concat('openedx_assignment_', ts.id::text)::text as id,

        x.student_id::text as student_id,

        -- Directly use the TAS assignment usage_key
        ts.usage_key::text as resource_id,

        null::text as mentor_id,

        ts.course_key::text as cohort_code,

        ts.status::text as submission_status,

        f.marks::numeric as marks,

        f.max_marks::numeric as max_marks,

        case
            when f.max_marks > 0 then
                round(
                    (f.marks / f.max_marks) * 100,
                    2
                )
            else null::numeric
        end as marks_pct,

        f.feedback_comments::text as feedback_comments,

        ts.submitted_at as submitted_at,

        ts.pdf::text as assignment_file,

        'Open edX'::text as source_system

    from {{ source('raw', 'openedx_tas_app_submission') }} ts

    inner join {{ ref('stg_student_id_xref') }} x
        on x.openedx_user_id = ts.student_id

    left join openedx_feedback f
        on f.submission_id = ts.id

)


select * from osot_rows
union all
select * from openedx_rows