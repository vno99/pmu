WITH courses AS (
    SELECT * FROM {{ ref('int_pmu__course') }}
),

participants AS (
    SELECT * FROM {{ ref('int_pmu__participant') }}
),


SELECT

    -- clés
    c.course_id_naturel AS course_id,
    p.participant_id_cheval,

    -- dimensions participant
    p.participant_num_pmu,
    -- p.participant_nom,
    p.participant_age,
    -- p.participant_sexe,
    -- p.participant_race,
    p.participant_entraineur,
    p.participant_driver,
    p.participant_driver_change,
    p.participant_deferre,
    p.participant_oeilleres,
    p.participant_musique,
    p.participant_statut,
    -- p.reunion_num,
    -- p.course_num_ordre         as participant_course_num_ordre,

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

    -- traçabilité
    -- c.source_file     as course_source_file,
    -- c.ingested_at     as course_ingested_at,
    -- p.source_file     as participant_source_file,
    -- p.ingested_at     as participant_ingested_at

FROM participants p 
LEFT JOIN courses c USING(course_id_naturel)
