-- Test singulier : un résultat d'arrivée ne doit jamais être associé à une date future.
-- Échoue (lignes retournées) si une course arrivée a une date > aujourd'hui.
SELECT race_id
FROM {{ ref('feature_store_horse_ranking_v3') }}
WHERE participant_ordre_arrivee IS NOT NULL
  AND course_date > CURRENT_DATE
