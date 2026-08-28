-- Test singulier : la date de programmation (course_date) extraite du nom de
-- fichier (date_str) doit correspondre exactement, pour chaque course.
-- course_date est calculé via to_date(date_str, 'YYYYMMDD') au staging : la
-- relation est strictement égale. L'ancienne sémantique "date de départ effectif"
-- (Europe/Paris, qui pouvait différer de ±1 jour pour les courses après minuit)
-- a été supprimée : course_date désigne le jour J du programme PMU.
-- Échoue (lignes retournées) si l'égalité n'est pas exacte.
SELECT course_id_naturel
FROM {{ ref('stg_raw__course') }}
WHERE course_date <> to_date(date_str, 'YYYYMMDD')
