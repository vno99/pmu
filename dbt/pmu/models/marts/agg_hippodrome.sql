WITH course_participant AS (
    SELECT * FROM {{ ref('fct_course_participant') }}
),

course AS (
    SELECT * FROM {{ ref('dim_course') }}
),


SELECT

    -- identifiant hippodrome (à enrichir si tu ajoutes hippodrome_code dans int_pmu__course)
    course_conditions AS conditions_course,
    course_discipline,
    course_distance,

    -- volumes
    count(DISTINCT c.course_id_naturel) AS nb_courses,
    count(DISTINCT date_str) AS nb_jours_course,
    count(*) AS nb_participations,

    -- partants
    round(avg(course_nb_declares_partants), 1) AS moy_partants_par_course,
    max(course_nb_declares_partants) AS max_partants,
    min(course_nb_declares_partants) AS min_partants,

    -- infos utiles
    min(course_date) AS premiere_course,
    max(course_date) AS derniere_course

FROM course_participant cp
    JOIN course c ON c.course_id_naturel = cp.course_id

GROUP BY
    course_conditions,
    course_discipline,
    course_distance
