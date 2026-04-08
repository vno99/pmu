-- DEPRECATED
{% macro filter_by_course_date(column_name) %}
    {% if var("current_date", none) is none %}
        1 = 1
    {% else %}
        {{ column_name }} = to_date('{{ var("current_date") }}', 'YYYYMMDD')
    {% endif %}
{% endmacro %}