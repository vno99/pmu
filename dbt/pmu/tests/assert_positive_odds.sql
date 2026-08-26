-- Test singulier : les cotes d'un participant doivent rester positives.
-- Échoue (lignes retournées) si une cote directe ou de référence est <= 0.
SELECT participant_course_id_naturel
FROM {{ ref('int_pmu__participant') }}
WHERE participant_cote_directe <= 0
   OR participant_cote_reference <= 0
