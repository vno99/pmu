-- Test singulier : la date dérivée du nom de fichier doit correspondre à la date de la course,
-- avec une tolérance d'±1 jour pour les courses partant juste après minuit
-- (départ réel le lendemain de la date de programmation).
-- Échoue (lignes retournées) si l'écart dépasse 1 jour.
SELECT course_id_naturel
FROM {{ ref('int_pmu__course') }}
WHERE abs(course_date - to_date(date_str, 'YYYYMMDD')) > 1
