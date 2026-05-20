-- Average prediction error (Error promedio)
SELECT AVG(prediction_error) AS mean_absolute_error
FROM fact_predictions;

-- Predictions by country (Predicciones por país)
SELECT 
    c.country_name, 
    AVG(f.predicted_score) as avg_predicted_score
FROM fact_predictions f
JOIN dim_country c ON f.country_id = c.country_id
GROUP BY c.country_name
ORDER BY avg_predicted_score DESC;

-- Predicted vs actual score (Predicho vs Real)
SELECT 
    c.country_name, 
    d.year, 
    f.actual_score, 
    f.predicted_score, 
    f.prediction_error
FROM fact_predictions f
JOIN dim_country c ON f.country_id = c.country_id
JOIN dim_date d ON f.date_id = d.date_id;

-- Prediction trends over time (Tendencia en el tiempo)
SELECT 
    d.year, 
    AVG(f.actual_score) as avg_actual_score, 
    AVG(f.predicted_score) as avg_predicted_score
FROM fact_predictions f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.year
ORDER BY d.year ASC;

-- Calidad de Datos 
SELECT processing_status, COUNT(*) as total_events
FROM raw_happiness_events
GROUP BY processing_status;