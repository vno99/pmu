{# 'fail' déclenche un faux positif : dbt-postgres crée les colonnes string en
   'varchar' alors que la vue source les expose en 'text' (types équivalents en
   PostgreSQL). 'append_new_columns' conserve la détection des colonnes ajoutées. #}
{{ config(
    materialized='incremental',
    unique_key=['course_id', 'participant_id_cheval'],
    on_schema_change='append_new_columns'
) }}

WITH courses AS (
    SELECT * FROM {{ ref('int_pmu__course') }}

    {% if is_incremental() %}
        WHERE {{ filter_course_date() }}
    {% endif %}
),

participants AS (
    SELECT * FROM {{ ref('int_pmu__participant', info=True) }}

    {% if is_incremental() %}
        WHERE {{ filter_course_date() }}
    {% endif %}
)

SELECT

    -- clés
    c.course_id_naturel AS course_id,
    p.participant_id_cheval,
    c.course_date,

    -- dimensions participant
    p.participant_num_pmu,
    p.participant_age,
    p.participant_entraineur,
    p.participant_driver,
    p.participant_driver_change,
    p.participant_deferre,
    p.participant_oeilleres,
    p.participant_musique,
    p.participant_statut,

    -- statistiques historiques
    p.participant_nombre_courses,
    p.participant_nombre_victoires,
    p.participant_nombre_places,
    p.participant_nombre_places_second,
    p.participant_nombre_places_troisieme,
    p.participant_taux_victoire,
    p.participant_taux_place,

    -- résultat
    p.participant_ordre_arrivee,
    p.participant_cote_directe,
    p.participant_cote_reference,
    p.participant_est_favori,
    p.is_gagnant,
    p.is_top_3

FROM participants p
LEFT JOIN courses c USING(course_id_naturel)
