-- Test singulier : chaque prédiction stockée dans dim_prediction doit avoir un score.
-- Le score LightGBM peut être négatif (score de gain, pas une probabilité) ;
-- la contrainte porte sur l'absence de NULL.
-- Échoue (lignes retournées) si un pred_score est NULL.
SELECT race_id
FROM {{ source('analytics_marts', 'dim_prediction') }}
WHERE pred_score IS NULL
