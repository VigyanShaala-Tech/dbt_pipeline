{{
  config(
    materialized='table',
    tags=['staging', 'student_registration_details']
  )
}}

with osot_rows as (

    select
        concat('OSOT_', id::text) as registration_id,
        student_id::text as student_id,
        assigned_through::text as assigned_through,
        form_details::jsonb as form_details,
        registration_date::timestamp as registration_date,
        inserted_at::timestamp as inserted_at,
        updated_at::timestamp as updated_at,
        'OSOT'::text as source_system

    from {{ source('raw', 'student_registration_details') }}

),

openedx_rows as (

    select
        concat('OPENEDX_', cfs.id::text) as registration_id,
        x.student_id::text as student_id,
        cast(null as text) as assigned_through,
        cfs.response_data::jsonb as form_details,
        cfs.created::timestamp as registration_date,
        cfs.created::timestamp as inserted_at,
        cfs.modified::timestamp as updated_at,
        'Open edX'::text as source_system

    from {{ source(
        'raw',
        'openedx_cohort_management_form_cohortformsubmission'
    ) }} cfs

    inner join {{ ref('stg_student_id_xref') }} x
        on x.openedx_user_id = cfs.user_id

)

select
    registration_id,
    student_id,
    assigned_through,
    form_details,
    registration_date,
    inserted_at,
    updated_at,
    source_system
from osot_rows

union all

select
    registration_id,
    student_id,
    assigned_through,
    form_details,
    registration_date,
    inserted_at,
    updated_at,
    source_system
from openedx_rows