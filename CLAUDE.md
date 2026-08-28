# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Vue d'ensemble

Pipeline de données (ETL) + Machine Learning pour les courses hippiques PMU. Le code et les commentaires sont en français.

Deux volets distincts :

1. **Data Engineering** — Apache Airflow 3.1.8 (CeleryExecutor) orchestre l'extraction des données brutes depuis l'API PMU (stockées d'abord en JSON sur disque), leur ingestion dans PostgreSQL (`raw`), puis leur transformation par **dbt** (staging → intermediate → marts).
2. **Inférence ML** — un DAG Airflow appelle une API **FastAPI** (hébergée sur Hugging Face) qui charge un modèle LightGBM de *ranking* depuis un serveur **MLflow** distant, score les features du feature store, puis sauvegarde les prédictions en base.

### Flux de données

```
API PMU (online.turfinfo.api.pmu.fr)
   → fichiers JSON dans data/pmu/{course,participant}/
   → tables raw.raw_course / raw.raw_participant (JSONB)
   → vues dbt stg_raw__* (dépliage JSON)
   → tables dbt int_pmu__course / int_pmu__participant
   → tables dbt mart : fct_course_participant, dim_*, agg_*, feature_store_horse_ranking_v3
   → API /predict (charge le modèle depuis MLflow, score le feature store)
   → analytics_marts.dim_prediction
```

## Commandes courantes

L'environnement s'exécute via Docker Compose (Airflow + 2× PostgreSQL + Redis). Le code Python seul (DAGs, services, tests) est exécuté dans les conteneurs Airflow ; dbt est installé dans l'image Airflow.

```bash
# Démarrer l'environnement complet
docker compose up -d

# UI Airflow : http://localhost:8080 (airflow / airflow)
# PostgreSQL de données : localhost:5433 (user / password, db data_db)
```

Tests unitaires (ne nécessitent pas Docker — ils testent `normalize_date`/`_file_date` dans `services/service_pmu.py`) :

```bash
pytest                # depuis la racine du projet
pytest tests/services/test_service_pmu.py
```

dbt (seulement exécutable dans le réseau Docker car `profiles.yml` pointe vers l'hôte `postgres-data`) :

```bash
# Dans un conteneur Airflow
docker compose exec airflow-scheduler dbt run --project-dir /opt/airflow/dbt/pmu --profiles-dir /opt/airflow/dbt/pmu --select <modèle>
docker compose exec airflow-scheduler dbt test --project-dir /opt/airflow/dbt/pmu --profiles-dir /opt/airflow/dbt/pmu --select <modèle>
```

Les DAGs passent `--vars '{"current_date": "YYYY-MM-DD"}'` pour filtrer les modèles incrémentaux sur la date du jour (voir « Formats de date »).

## Orchestration Airflow (dags/)

- `pmu_daily_call` — **orchestrateur quotidien** (03:00) : appelle l'API PMU (`_get_data`), puis déclenche en chaîne `pmu_daily_insert_raw` → `pmu_dbt_raw_to_intermediate` → `pmu_dbt_int_to_mart` via `TriggerDagRunOperator` (deferrable, `wait_for_completion=True`).
- `pmu_daily_insert_raw` — lit les fichiers JSON du jour depuis `/data/pmu/{course,participant}`, les charge dans `raw.raw_course` / `raw.raw_participant` par lots de 2000, upsert sur `source_file` (incrémente `import_count`).
- `pmu_dbt_raw_to_intermediate` — lance `dbt run`+`dbt test` sur staging puis intermediate.
- `pmu_dbt_int_to_mart` — lance `dbt run`+`dbt test` sur les marts (dont `feature_store_horse_ranking_v3`). Supporte un conf `full_refresh` → `--full-refresh`.
- `pmu_daily_predict` — (04:00) appelle l'API `/predict` et insère/remplace les prédictions dans `analytics_marts.dim_prediction` (clé `race_id, horse_id`).
- `pmu_daily_result` — (02:00) calcule la date de la veille et déclenche `pmu_daily_call` avec `current_date`.
- `pmu_full_insert_raw` — **backfill manuel** : charge tous les JSON de `data/pmu/` dans `raw`, dédoublonnage sur `file_hash` (`ON CONFLICT DO NOTHING`).
- `pmu_create_raw_tables` (dag_id `pmu_init_schema`) — exécute `sql/create_raw_tables.sql` pour initialiser le schéma `raw`.

Fonctions transverses partagées dans `services/service_pmu.py` : `normalize_date` (validation/conversion ISO), `_file_date` (frontière ISO → `YYYYMMDD` pour les noms de fichiers), `_get_data` (appel API PMU + écriture des JSON), `fetch_course_pmu`, `fetch_participants_pmu`, `_get_reunions_courses`.

## Couches dbt (dbt/pmu/)

`profiles.yml` : schéma racine `analytics`, connexion Postgres `postgres-data` (db `data_db`). Matérialisations définies dans `dbt_project.yml` : staging = vues, intermediate = tables (index sur `course_date`), marts = tables.

- **staging/raw** — `stg_raw__*.sql` : déplie le JSONB (`jsonb_array_elements`) et extrait les champs typés ; construit les identifiants naturels (`course_id_naturel` = `{date}_R{r}C{c}`, `participant_course_id_naturel` = `..._P{numPmu}`).
- **intermediate** — `int_pmu__course` (convertit `heureDepart` epoch → `course_heure_depart_ts`/`course_date`), `int_pmu__participant` (normalise les noms père/mère via `unaccent`, calcule `participant_taux_victoire/place`, `is_gagnant`, `is_top_3`, construit `participant_id_cheval` avec repli sur nom+mère+père). Modèles **incrémentaux** avec `unique_key` et contrat de données (`on_schema_change='append_new_columns'`, voir « Points d'attention »).
- **marts** — `fct_course_participant` (jointure course/participant, table de faits centrale), `dim_course`, `dim_participant` (`DISTINCT ON (participant_id_cheval)`), `agg_driver`, `agg_entraineur`, `agg_hippodrome`.
- **feature store** — `feature_store_horse_ranking_v3.sql` (actif) : modèle incrémental alimenté par la v3 qui enrichit `fct_course_participant` avec des fenêtres glissantes (avg finish last3/5, win-rate driver/trainer last20, lag distance/discipline, parsing de la « musique » du cheval, features relatives à la course). `feature_store_horse_ranking_v1.sql` est l'ancienne version sans parsing musique. La liste exacte des colonnes servies au modèle est le contrat entre le feature store et l'API.

## API d'inférence (api/)

`api/app.py` — FastAPI. Au démarrage (lifespan) : charge le modèle LightGBM `ranker_v5_hybrid` depuis MLflow (`MLFLOW_URI`, run `RUN_ID`) plus le métadata `metadata/model_features.json` (contient `feature_cols` et `categorical_cols`). L'endpoint `POST /predict` attend `{"input": "YYYY-MM-DD"}`, lit le feature store pour cette date (`course_date`), caste les colonnes selon le métadata et renvoie les prédictions triées par `pred_score` décroissant.

Variables d'environnement requises (`api/.env`, **non versionné**) : `MLFLOW_URI`, `DB_URL`, `RUN_ID`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`. Le serveur MLflow est hébergé sur HF Space (`https://jiro99-mlflow2.hf.space/`).

## Entraînement du modèle (training/)

Expérimentations LightGBM Ranker dans les notebooks `pred_v1.ipynb` … `pred_v5.ipynb` et scripts `pred.py` / `pred_inf.py`. Chaque version produit `metrics_ranker_vX.csv`, `feature_importance_ranker_vX.csv`, `top20_importance_vX.csv`, `predictions_ranker_vX.csv`. Le modèle en production est `ranker_v5_hybrid`, loggé dans MLflow sous l'expérience `pmu` avec ses features.

## Points d'attention

- **Formats de date** — format canonique **ISO `YYYY-MM-DD`** pour le paramètre de DAG `current_date`, l'API `/predict` et la var dbt `current_date`. Côté Python, `normalize_date()` valide/convertit (sentinelles `None`/`""` → date du jour) ; en dbt, `{{ filter_course_date() }}` applique le filtre incrémental sur la colonne `course_date`. `course_date` (type `date`) = **date de programmation** du programme PMU (celle du nom de fichier). Les **noms de fichiers JSON** restent en `YYYYMMDD` (conversion via `_file_date()`), les **URLs de l'API PMU** en `DDMMYYYY`, et les identifiants naturels (`course_id_naturel`) en `YYYYMMDD`.
- **Schémas** — dbt concatène le schéma du profil (`analytics` dans `profiles.yml`) avec le `+schema` de chaque couche : les schémas réels sont `analytics_staging`, `analytics_intermediate`, `analytics_marts` (pas `analytics.marts` avec un point). L'API et le DAG predict utilisent donc bien `analytics_marts.*` (feature store et `dim_prediction`, créée par `sql/create_predictions_table.sql`).
- **Fuseau horaire** — les DAGs sont en `Europe/Paris` ; un bug historique a été corrigé pour les courses partant juste après minuit (attention aux conversions de date autour de minuit).
- **Colonnes `varchar` vs `text`** — dbt-postgres crée les colonnes string des tables incrémentales en `varchar` alors que les vues staging les exposent en `text`. Types équivalents en PostgreSQL, mais `on_schema_change='fail'` les voyait comme un changement et bloquait tout run incrémental ; les modèles incrémentaux utilisent donc `on_schema_change='append_new_columns'` (la détection des colonnes ajoutées reste active).
- **RAW vs prédictions** — les tables `raw.*` sont initialisées par `sql/create_raw_tables.sql` (DAG `pmu_init_schema`) ; la table de prédiction est créée par `sql/create_predictions_table.sql`. La connexion Airflow `data_db` (pointant vers `postgres-data`) doit être configurée dans l'UI (Admin > Connections).
- **Secrets** — `api/.env` contient des credentials AWS et MLflow ; jamais les committer (`.gitignore` exclut `.env`). N'ajouter des secrets que dans les `.env` locaux.
