WITH src AS (
    SELECT * FROM {{ ref('int_pmu__course') }}
)

SELECT

    -- clé primaire
    course_id_naturel,

    -- dimensions temporelles
    date_str,
    course_date,
    course_heure_depart_ts,

    -- identifiants réunion / course
    reunion_num_officiel,
    course_num_ordre,

    -- caractéristiques course
    course_libelle,
    course_discipline,
    course_distance,
    course_distance_unit,
    course_statut,
    course_corde,
    course_parcours,
    course_conditions,
    course_nb_declares_partants,

    -- JSON brut si besoin d'enrichissement futur
    course_paris_json,
    course_ordre_arrivee_json,

    -- traçabilité
    source_file

FROM src
