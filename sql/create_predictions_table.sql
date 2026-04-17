CREATE SCHEMA IF NOT EXISTS analytics_marts;

CREATE TABLE IF NOT EXISTS analytics_marts.dim_prediction (
    race_id VARCHAR(50) NOT NULL,
    horse_id INT NOT NULL,
    participant_num_pmu INT,
    pred_score NUMERIC(10, 5),
    prediction_date DATE NOT NULL,
    model_run VARCHAR(50) NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (race_id, horse_id)
);
