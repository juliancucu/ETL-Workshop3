-- 1. Tabla Raw (Auditoría)
CREATE TABLE IF NOT EXISTS raw_happiness_events (
    raw_event_id SERIAL PRIMARY KEY,
    kafka_offset BIGINT,
    payload_json JSONB,
    processing_status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Dimensiones
CREATE TABLE IF NOT EXISTS dim_country (
    country_id SERIAL PRIMARY KEY,
    country_name VARCHAR(100) UNIQUE NOT NULL -- ¡El UNIQUE es vital para tu ON CONFLICT!
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id SERIAL PRIMARY KEY,
    year INT UNIQUE NOT NULL -- ¡El UNIQUE es vital para tu ON CONFLICT!
);

-- 3. Tabla de Hechos
CREATE TABLE IF NOT EXISTS fact_predictions (
    prediction_id SERIAL PRIMARY KEY,
    raw_event_id INT REFERENCES raw_happiness_events(raw_event_id),
    country_id INT REFERENCES dim_country(country_id),
    date_id INT REFERENCES dim_date(date_id),
    actual_score NUMERIC(10, 4),
    predicted_score NUMERIC(10, 4),
    prediction_error NUMERIC(10, 4),
    prediction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- ¡Así cumple el requisito del PDF sin enviarlo desde Python!
);