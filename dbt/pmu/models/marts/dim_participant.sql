WITH src AS (
    SELECT * FROM {{ ref('int_pmu__participant') }}
),


SELECT DISTINCT ON (participant_id_cheval)

    -- clé primaire
    participant_id_cheval,

    -- caractéristiques stables du cheval
    participant_nom,
    participant_sexe,
    participant_race,
    participant_nom_pere_normalized,
    participant_nom_mere_normalized,

    -- traçabilité
    source_file

FROM src
ORDER BY participant_id_cheval, ingested_at DESC
