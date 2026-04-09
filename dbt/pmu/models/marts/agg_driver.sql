WITH course_participant AS (
    SELECT * FROM {{ ref('fct_course_participant') }}
),

course AS (
    SELECT * FROM {{ ref('dim_course') }}
)

SELECT

    participant_driver AS driver,

    -- volumes
    count(*) AS nb_participations,
    count(DISTINCT c.course_date) AS nb_jours_course,

    -- performances
    sum(CASE WHEN is_gagnant THEN 1 ELSE 0 END) AS nb_victoires,
    sum(CASE WHEN is_top_3   THEN 1 ELSE 0 END) AS nb_top_3,

    -- taux
    round(
        sum(CASE WHEN is_gagnant THEN 1 ELSE 0 END)::numeric
        / nullif(count(*), 0) * 100, 2
    ) AS taux_victoire_pct,

    round(
        sum(CASE WHEN is_top_3 THEN 1 ELSE 0 END)::numeric
        / nullif(count(*), 0) * 100, 2
    ) AS taux_top3_pct,

    -- infos utiles
    min(c.course_date) AS premiere_course,
    max(c.course_date) AS derniere_course,

    -- discipline principale
    mode() WITHIN group (ORDER BY course_discipline) AS discipline_principale

FROM course_participant cp
    JOIN course c ON c.course_id_naturel = cp.course_id

WHERE participant_driver IS NOT NULL

GROUP BY participant_driver
