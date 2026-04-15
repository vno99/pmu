# PMU Pipeline

![Python](https://img.shields.io/badge/Python-3.10+-blue.svg?logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-3.1.8-017CEE.svg?logo=Apache%20Airflow&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-Core-FF694B.svg?logo=dbt&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16+-316192.svg?logo=postgresql&logoColor=white)
![MLflow](https://img.shields.io/badge/MLflow-0194E2.svg?logo=MLflow&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-009688.svg?logo=FastAPI&logoColor=white)
![Hugging Face](https://img.shields.io/badge/Hugging%20Face-Spaces-F59A23.svg?logo=Hugging%20Face&logoColor=white)

Pipeline de données (ETL) développé pour l'ingénierie, la transformation et la prédiction des courses hippiques.  
Ce projet orchestre la récupération des données brutes, leur transformation via dbt, et applique un modèle de Machine Learning de type Ranking (LightGBM), stocké sur un serveur MLflow distant, pour prédire l'ordre d'arrivée des chevaux.

## Architecture du projet

Le projet s'articule autour de **deux parties distinctes** :  

### 1. Data Engineering (Pipeline ETL)
Géré par Apache Airflow, PostgreSQL et dbt.
- **Extraction** : Récupération des données brutes des courses du jour via API et stockage dans PostgreSQL (`raw_`).
- **Transformation** : Nettoyage, typage strict, et calcul des features prédictives via **dbt**

### 2. Inférence (Machine Learning via API)
Gérée par Airflow qui orchestre l'appel à une API prédictive et la sauvegarde des résultats.
- Une fois le DAG de mise à jour quotidienne (ETL) terminé avec succès, un **second DAG Airflow dédié** prend le relais.
- Ce DAG fait un appel à l'API d'inférence.
- L'API charge le modèle de *Ranking* (LightGBM) depuis MLflow, score les features préparées par dbt, et renvoie les prédictions (classement estimé des chevaux).
- Le DAG Airflow récupère cette réponse API et **sauvegarde les prédictions finales directement dans la base de données PostgreSQL**.

## 📁 Structure du projet
```text
.
├── api/                        # Ensemble des endpoints de l'API
├── config/
│   └── airflow.cfg             # Configuration de Apache Airflow
├── dags/                       # Ensemble des DAGs Apache Airflow (ex: pmu_daily_call.py)
├── data/                       # Dossier de stockage des fichiers JSON
│   └── pmu/
│       ├── course/
│       └── participant/
├── dbt/
│   └── pmu/
│       ├── models/
│       │   ├── intermediate/   # Tables intermédiaires (int_)
│       │   ├── marts/          # Feature store & dimensions (fct_, dim_)
│       │   ├── staging/        # Vues brutes (stg_)
│       │   └── sources.yml     # Déclaration des tables raw
│       ├── dbt_project.yml     # Configuration globale dbt
│       └── profiles.yml        # Connexion PostgreSQL
├── services/                   # Scripts et utilitaires Python transverses
├── sql/                        # Scripts SQL
├── docker-compose.yml          # Environnement local (Airflow, PostgreSQL)
└── README.md
```

## Orchestration Airflow & Flux d'exécution
L'automatisation quotidienne (programmée à 3h00) suit ce flux :

1. **DAG `pmu_daily_call`** :
   - Extrait les données fraîches des courses du jour.
   - Met à jour les tables intermediate, mart
2. **DAG `pmu_daily_inference`** :
   - Se déclenche automatiquement à la fin de l'ETL.
   - Fait appel à l'API de prédiction.
   - Parse le JSON de retour de l'API.
   - Insère les classements prédits dans une table dédiée.

## Installation & Démarrage (Local)

### Prérequis
- Docker Compose v2
- Le serveur MLflow est hébergé dans Hugging Face (s'assurer que le Space est bien démarré)
https://jiro99-mlflow2.hf.space/
- L'API d'inférence est hébergée dans Hugging Face (s'assurer qu'elle est opérationnelle)

### Configuration Airflow
Avant de lancer les DAGs, il est nécessaire de configurer la connexion à la base de données dans l'interface web d'Airflow (Admin > Connections) :

- **Connection ID** : `data_db`
- **Connection Type** : `Postgres`
- **Host** : `postgres-data`
- **Login** : `user`
- **Port** : `5432`
- **Database** : `data_db`

### Démarrer le projet
Lancez les bases de données PostgreSQL et Apache Airflow en arrière-plan :
```bash
docker compose up -d
```
