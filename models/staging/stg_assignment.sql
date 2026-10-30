SELECT
    t.id,
    t.name,
    t.image,
    t.fields,
    t.created,
    t.modified,
    t.is_active,
    t.is_public,
    t.description,
    t.created_by_id,
    t.template_type_id,

    tb.id AS templateblock_id,
    tb.created AS templateblock_created,
    tb.modified AS templateblock_modified,
    tb.rubric_id AS templateblock_rubric_id,
    tb.usage_key AS templateblock_usage_key,
    tb.course_key AS templateblock_course_key,
    tb.sort_order AS templateblock_sort_order,
    tb.assigned_at AS templateblock_assigned_at,
    tb.template_id AS templateblock_template_id,
    tb.display_name AS templateblock_display_name,
    tb.instructions AS templateblock_instructions,
    tb.feedback_options AS templateblock_feedback_options

FROM raw."Openedx_tas_app_template" t

INNER JOIN raw."Openedx_tas_app_templateblock" tb
    ON t.id = tb.template_id;