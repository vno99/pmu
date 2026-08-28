-- Filtre incrémental unique par date de programmation.
-- La colonne canonique `course_date` est de type `date` (jour J du programme PMU,
-- celui du nom de fichier). La var `current_date` est au format ISO (YYYY-MM-DD) :
-- le cast `::date` est donc direct, sans `to_date(..., 'YYYYMMDD')`.
-- Sans var `current_date` (run non daté, ex : backfill), aucun filtre (1 = 1).
{% macro filter_course_date(column_name='course_date') %}
    {% if var("current_date", none) is none %}
        1 = 1
    {% else %}
        {{ column_name }} = '{{ var("current_date") }}'::date
    {% endif %}
{% endmacro %}
