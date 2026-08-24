{{
  config(
    materialized='table',
    tags=['staging', 'identity']
  )
}}

-- Historical OSOT students, normalized for MATCHING only (not stored back).
with osot_normalized as (

    select
        id                                             as student_id,
        email                                          as osot_email_raw,
        phone                                          as osot_phone_raw,
        lower(trim(email))                             as email_norm,
        regexp_replace(phone, '[^0-9+]', '', 'g')       as phone_norm

    from {{ source('raw', 'student_details') }}

),

openedx_normalized as (

    select
        au.id                                          as openedx_user_id,
        au.email                                       as openedx_email_raw,
        up.phone_number                                as openedx_phone_raw,
        lower(trim(au.email))                          as email_norm,
        regexp_replace(up.phone_number, '[^0-9+]', '', 'g') as phone_norm

    from {{ source('raw', 'openedx_auth_user') }} au
    left join {{ source('raw', 'openedx_auth_userprofile') }} up
        on up.user_id = au.id

),

email_matches as (

    select
        oe.openedx_user_id,
        o.student_id,
        'email'               as matched_by,
        oe.openedx_email_raw  as matched_value,
        count(*) over (partition by oe.email_norm) as email_norm_hits,
        count(*) over (partition by o.email_norm)  as osot_email_norm_hits

    from openedx_normalized oe
    inner join osot_normalized o
        on oe.email_norm = o.email_norm
        and oe.email_norm is not null

),

phone_matches as (

    select
        oe.openedx_user_id,
        o.student_id,
        'phone'               as matched_by,
        oe.openedx_phone_raw  as matched_value,
        count(*) over (partition by oe.phone_norm) as phone_norm_hits,
        count(*) over (partition by o.phone_norm)  as osot_phone_norm_hits

    from openedx_normalized oe
    inner join osot_normalized o
        on oe.phone_norm = o.phone_norm
        and oe.phone_norm is not null
    where oe.openedx_user_id not in (select openedx_user_id from email_matches)

),

resolved_matches as (

    select openedx_user_id, student_id, matched_by, matched_value
    from email_matches
    where email_norm_hits = 1 and osot_email_norm_hits = 1

    union all

    select openedx_user_id, student_id, matched_by, matched_value
    from phone_matches
    where phone_norm_hits = 1 and osot_phone_norm_hits = 1

),

-- Ambiguous matches routed to a separate conflicts table, never guessed.
conflicts as (

    select openedx_user_id, student_id, matched_by, matched_value
    from email_matches
    where email_norm_hits > 1 or osot_email_norm_hits > 1

    union all

    select openedx_user_id, student_id, matched_by, matched_value
    from phone_matches
    where phone_norm_hits > 1 or osot_phone_norm_hits > 1

),

final as (

    select
        coalesce(
            cast(r.student_id as text),
            'openedx_' || oe.openedx_user_id::text
        ) as student_id,

        oe.openedx_user_id,

        coalesce(r.matched_by, 'unmatched') as matched_by,
        r.matched_value,
        current_timestamp as matched_at,
        true as is_current

    from openedx_normalized oe

    left join resolved_matches r
        on oe.openedx_user_id = r.openedx_user_id

    where oe.openedx_user_id not in (
        select openedx_user_id
        from conflicts
    )

)

select * from final