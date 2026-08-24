{{
  config(
    materialized='table',
    tags=['staging', 'student_session']
  )
}}

with osot_rows as (

    select
        id::text as id,
        student_id::text as student_id,
        session_id::text as session_id,
        duration_in_sec::integer as duration_in_sec,
        watched_on,
        'OSOT'::text as source_system

    from {{ source('raw', 'student_session') }}

),

-- Extract each attendee from the Zoom occurrence attendance_report JSON.
occurrence_attendees as (

    select
        o.internal_id::text as session_id,

        o.start_time,

        lower(trim(participant.value ->> 'email'))
            as attendee_email_norm,

        participant.value ->> 'status'
            as status,

        case
            when participant.value ->> 'duration' ~ '^[0-9]+'
                then regexp_replace(
                    participant.value ->> 'duration',
                    '[^0-9]',
                    '',
                    'g'
                )::integer * 60
            else 0
        end as duration_in_sec,

        participant.value ->> 'join_time'
            as join_time_raw,

        participant.value ->> 'leave_time'
            as leave_time_raw

    from {{ source('raw', 'openedx_zoom_integration_occurrences') }} o

    left join lateral
        jsonb_array_elements(
            coalesce(
                o.attendance_report::jsonb -> 'participants',
                '[]'::jsonb
            )
        ) as participant(value)
        on true

),

-- Match Zoom attendee email with Open edX auth_user.
matched_users as (

    select
        oa.*,
        au.id as openedx_user_id

    from occurrence_attendees oa

    left join {{ source('raw', 'openedx_auth_user') }} au
        on lower(trim(au.email)) = oa.attendee_email_norm

),

openedx_rows as (

    select
        concat(
            'openedx_session_',
            mu.session_id,
            '_',
            mu.openedx_user_id::text
        )::text as id,

        x.student_id::text as student_id,

        mu.session_id::text as session_id,

        mu.duration_in_sec::integer as duration_in_sec,

        (
            mu.start_time::date
            + to_timestamp(mu.join_time_raw, 'HH12:MI AM')::time
        ) as watched_on,

        'Open edX'::text as source_system

    from matched_users mu

    inner join {{ ref('stg_student_id_xref') }} x
        on x.openedx_user_id = mu.openedx_user_id

    where mu.openedx_user_id is not null

)

select * from osot_rows

union all

select * from openedx_rows