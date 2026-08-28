{# 'fail' déclenche un faux positif : dbt-postgres crée les colonnes string en
   'varchar' alors que la vue source les expose en 'text' (types équivalents en
   PostgreSQL). 'append_new_columns' conserve la détection des colonnes ajoutées. #}
{{ config(
    materialized='incremental',
    unique_key=['course_id_naturel'],
    on_schema_change='append_new_columns'
) }}

WITH src AS (
    SELECT * FROM {{ ref('stg_raw__course') }}

    {% if is_incremental() %}
        WHERE {{ filter_course_date() }}
    {% endif %}
)

SELECT
    course_id_naturel,
    raw_id,
    source_file,
    file_hash,
    ingested_at,
    date_str,
    to_timestamp(course_heure_depart / 1000.0) AS course_heure_depart_ts,
    course_date,
    reunion_num_officiel,
    reunion_num_externe,
    course_num_reunion,
    course_num_ordre,
    course_num_externe,
    course_libelle,
    course_discipline,
    course_distance,
    course_distance_unit,
    course_nb_declares_partants,
    course_statut,
    course_corde,
    course_parcours,
    course_conditions,
    course_paris_json, 
    course_ordre_arrivee_json,
    course_json

FROM src