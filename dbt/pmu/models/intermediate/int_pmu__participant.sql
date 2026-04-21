{{ config(
    materialized='incremental',
    unique_key=['participant_course_id_naturel'],
    on_schema_change='fail'
) }}

WITH src AS (
    SELECT * FROM {{ ref('stg_raw__participant') }}

    {% if is_incremental() %}
        WHERE course_date = '{{ var("current_date", modules.datetime.date.today() | string) }}'::DATE
    {% endif %}
),

pere_mere_normalized AS (
    SELECT
        participant_course_id_naturel,
        COALESCE(
            NULLIF(
                TRIM(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                unaccent(UPPER(participant_nom_pere)),
                                '\s*\([^)]*\)', '', 'g'  -- retire (ITY), (FR)...
                            ),
                            '\s*-\s*', '-', 'g'          -- normalise les tirets
                        ),
                        '\s+', ' ', 'g'                  -- espaces multiples
                    )
                ),
            ''),
        'UNKNOWN') AS participant_nom_pere_normalized,
        COALESCE(
            NULLIF(
                TRIM(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                unaccent(UPPER(participant_nom_mere)),
                                '\s*\([^)]*\)', '', 'g'  -- retire (ITY), (FR)...
                            ),
                            '\s*-\s*', '-', 'g'          -- normalise les tirets
                        ),
                        '\s+', ' ', 'g'                  -- espaces multiples
                    )
                ),
            ''),
        'UNKNOWN') AS participant_nom_mere_normalized

    FROM src
)

SELECT
    participant_course_id_naturel,
    course_id_naturel,
    raw_id,
    source_file,
    file_hash,
    ingested_at,
    course_date,
    reunion_num,
    course_num_ordre,
    participant_num_pmu,
    coalesce(
        participant_id_cheval,
        nullif(trim(participant_nom), '') || '-'
            || trim(participant_nom_mere_normalized) || '-'
            || trim(participant_nom_pere_normalized)
    ) AS participant_id_cheval,
    participant_nom,
    participant_age,
    participant_sexe,
    participant_nom_mere_normalized,
    participant_nom_pere_normalized,
    participant_race,
    participant_entraineur,
    participant_driver,
    participant_driver_change,
    participant_deferre,
    participant_oeilleres,
    participant_musique,
    participant_statut,
    participant_nombre_courses,
    participant_nombre_victoires,
    participant_nombre_places,
    participant_nombre_places_second,
    participant_nombre_places_troisieme,
    participant_ordre_arrivee,
    participant_cote_directe,
    participant_cote_reference,
    participant_est_favori,

    CASE
        WHEN participant_nombre_courses > 0
            THEN participant_nombre_victoires::numeric / participant_nombre_courses
        ELSE null
    END AS participant_taux_victoire,

    CASE
        WHEN participant_nombre_courses > 0
            THEN participant_nombre_places::numeric / participant_nombre_courses
        ELSE null
    END AS participant_taux_place,

    CASE
        WHEN participant_ordre_arrivee = 1 THEN true
        ELSE false
    END AS is_gagnant,

    CASE
        WHEN participant_ordre_arrivee IS NOT NULL AND participant_ordre_arrivee <= 3 THEN true
        ELSE false
    END AS is_top_3,

    participant_json

FROM src s
JOIN pere_mere_normalized pmn USING(participant_course_id_naturel)