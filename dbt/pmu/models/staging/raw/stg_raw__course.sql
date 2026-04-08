WITH source AS (
    SELECT
        id,
        source_file,
        file_hash,
        ingested_at,
        substring(lower(source_file) FROM '.*/([0-9]{8})_course') AS date_str,
        json_data
    FROM {{ source('raw', 'raw_course') }}
),

reunions AS (
    SELECT
        id,
        source_file,
        file_hash,
        ingested_at,
        date_str,
        to_date(date_str, 'YYYYMMDD')::text AS course_date,
        -- to_timestamp((programme_date + programme_timezone_offset) / 1000.0)::date as programme_date_dt,
        jsonb_array_elements(json_data -> 'programme' -> 'reunions') AS reunion
    FROM source
),

courses AS (
    SELECT
        id,
        source_file,
        file_hash,
        ingested_at,
        date_str,
        course_date,
        reunion,
        -- programme_date_dt,
        jsonb_array_elements(reunion -> 'courses') AS course
    FROM reunions
)

SELECT
    id AS raw_id,
    source_file,
    file_hash,
    ingested_at,
    date_str,
    (reunion ->> 'numOfficiel')::int AS reunion_num_officiel,
    (reunion ->> 'numExterne')::int AS reunion_num_externe,
    reunion ->> 'nature' AS reunion_nature,
    reunion -> 'hippodrome' ->> 'code' AS reunion_hippodrome_code,
    reunion -> 'hippodrome' ->> 'libelleCourt' AS reunion_hippodrome_libelle_court,
    reunion -> 'pays' ->> 'code' AS reunion_pays_code,
    reunion -> 'pays' ->> 'libelle' AS reunion_pays_libelle,
    (course ->> 'numReunion')::int AS course_num_reunion,
    (course ->> 'numOrdre')::int AS course_num_ordre,
    (course ->> 'numExterne')::int AS course_num_externe,
    (course ->> 'heureDepart')::bigint AS course_heure_depart,
    course ->> 'libelle' AS course_libelle,
    course ->> 'libelleCourt' AS course_libelle_court,
    (course ->> 'montantPrix')::numeric AS course_montant_prix,
    (course ->> 'parcours') AS course_parcours,
    (course ->> 'distance')::int AS course_distance,
    course ->> 'distanceUnit' AS course_distance_unit,
    course ->> 'corde' AS course_corde,
    course ->> 'discipline' AS course_discipline,
    course ->> 'specialite' AS course_specialite,
    course ->> 'categorieParticularite' AS course_categorie_particularite,
    course ->> 'conditionAge' AS course_condition_age,
    course ->> 'conditionSexe' AS course_condition_sexe,
    (course ->> 'nombreDeclaresPartants')::int AS course_nb_declares_partants,
    (course ->> 'montantTotalOffert')::numeric AS course_montant_total_offert,
    (course ->> 'montantOffert1er')::numeric AS course_montant_offert_1er,
    (course ->> 'montantOffert2eme')::numeric AS course_montant_offert_2eme,
    (course ->> 'montantOffert3eme')::numeric AS course_montant_offert_3eme,
    (course ->> 'montantOffert4eme')::numeric AS course_montant_offert_4eme,
    (course ->> 'montantOffert5eme')::numeric AS course_montant_offert_5eme,
    course ->> 'conditions' AS course_conditions,
    (course ->> 'numCourseDedoublee')::int AS course_num_course_dedoublee,
    course ->> 'statut' AS course_statut,
    course ->> 'categorieStatut' AS course_categorie_statut,
    (course ->> 'dureeCourse')::int AS course_duree_course,
    (course ->> 'rapportsDefinitifsDisponibles')::boolean AS course_rapports_definitifs_disponibles,
    (course ->> 'isArriveeDefinitive')::boolean AS course_is_arrivee_definitive,
    (course ->> 'isDepartImminent')::boolean AS course_is_depart_imminent,
    (course ->> 'replayDisponible')::boolean AS course_replay_disponible,
    course -> 'hippodrome' ->> 'codeHippodrome' AS course_hippodrome_code,
    course -> 'hippodrome' ->> 'libelleCourt' AS course_hippodrome_libelle_court,
    (course ->> 'hasEParis')::boolean AS course_has_eparis,
    course -> 'paris' AS course_paris_json,
    course -> 'ordreArrivee' AS course_ordre_arrivee_json,
    concat(course_date, '_R', course ->> 'numReunion', 'C', course ->> 'numOrdre') AS course_id_naturel,
    course AS course_json
FROM courses
