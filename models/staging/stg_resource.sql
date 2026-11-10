{{
  config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    merge_exclude_columns=['category', 'title', 'description'],
    tags=['staging', 'resource']
  )
}}

WITH osot_rows AS (

    SELECT
        id::text AS id,
        category::text AS category,
        title::text AS title,
        description::text AS description,
        location::text AS location,
        resource_link::text AS resource_link,
        is_video_resource::boolean AS is_video_resource,
        total_duration::integer AS total_duration,
        'OSOT'::text AS source_system

    FROM {{ source('raw', 'resource') }}

    {% if is_incremental() %}
    WHERE id::text NOT IN (
        SELECT id FROM {{ this }}
    )
    {% endif %}

),

openedx_catalog AS (

    SELECT
        cb.*,
        ROW_NUMBER() OVER (
            PARTITION BY cb.usage_key
            ORDER BY cb.modified DESC
        ) AS rn

    FROM {{ source('raw', 'Openedx_course_content_catalog_coursecontentblock') }} cb

),

openedx_mapped AS (

    SELECT
        usage_key::text AS id,
        unit_name::text AS title,
        section_name::text AS description,
        block_type::text AS category

    FROM openedx_catalog
    WHERE rn = 1

),

openedx_rows AS (

    SELECT
        id,
        category,
        title,
        description,
        NULL::text AS location,
        NULL::text AS resource_link,
        (category = 'video') AS is_video_resource,
        NULL::integer AS total_duration,
        'Open edX'::text AS source_system

    FROM openedx_mapped

    {% if is_incremental() %}
    WHERE id NOT IN (
        SELECT id FROM {{ this }}
    )
    {% endif %}

)

SELECT * FROM osot_rows
UNION ALL
SELECT * FROM openedx_rows