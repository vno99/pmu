CREATE SCHEMA IF NOT EXISTS raw;

-- RAW courses
CREATE TABLE IF NOT EXISTS raw.raw_course (
    id BIGSERIAL  PRIMARY KEY,
    source_file TEXT NOT NULL,
    file_hash TEXT,
    json_data JSONB,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    import_count INT DEFAULT 1,
    CONSTRAINT uq_raw_course_source_file UNIQUE (source_file)
);

-- RAW participants
CREATE TABLE IF NOT EXISTS raw.raw_participant (
    id BIGSERIAL  PRIMARY KEY,
    source_file TEXT NOT NULL,
    file_hash TEXT,
    json_data JSONB,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    import_count INT DEFAULT 1,
    CONSTRAINT uq_raw_participant_source_file UNIQUE (source_file)
);

CREATE EXTENSION IF NOT EXISTS unaccent;