import json
import os
import joblib
import psycopg2
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'happiness-predictions'
DB_PARAMS = {
    "host": "localhost",
    "database": "happiness_db",
    "user": "happiness_user",
    "password": "happiness_password",
    "port": 5432
}

def validate_incoming_event(payload):
    expected_fields = ["country", "year", "gdp", "family", "health", "freedom", "generosity", "corruption", "actual_happiness_score"]
    
    # 1. Validación de Esquema (Campos Faltantes)
    for field in expected_fields:
        if field not in payload:
            return "INVALID_SCHEMA", f"Missing field: {field}"
            
    # 2. Validación de Tipos Básicos
    if not isinstance(payload["country"], str) or not isinstance(payload["year"], int):
        return "INVALID_SCHEMA", "Data type validation failed on keys"
        
    # 3. Validación de Rangos Numéricos Lógicos
    numeric_features = ["gdp", "family", "health", "freedom", "generosity", "corruption", "actual_happiness_score"]
    for feat in numeric_features:
        try:
            val = float(payload[feat])
            if val < 0 or val > 15: # Las métricas de felicidad nunca son negativas
                return "INVALID_VALUES", f"Out of bounds detected in field {feat}: {val}"
        except (ValueError, TypeError):
            return "INVALID_SCHEMA", f"Field {feat} is not castable to float"
            
    return "VALID", None

def run_consumer():
    # Conexión persistente a la base de datos relacional de Docker
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    
    # Carga segura del modelo predictivo serializado
    model_path = "models/model.pkl"
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"Debe entrenar el modelo antes de arrancar el Consumidor. Falta: {model_path}")
    model = joblib.load(model_path)
    
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id='grupo_julian'
    )
    
    print("🔥 Kafka Consumer conectado. Escuchando eventos entrantes en tiempo real...")
    
    for message in consumer:
        payload = message.value
        offset = message.offset
        
        # Persistir de forma inmediata en la tabla RAW (Auditoría/Trazabilidad obligatoria)
        status, debug_reason = validate_incoming_event(payload)
        
        try:
            cursor.execute(
                """
                INSERT INTO raw_happiness_events (kafka_offset, payload_json, processing_status)
                VALUES (%s, %s, %s) RETURNING raw_event_id;
                """,
                (offset, json.dumps(payload), status)
            )
            raw_event_id = cursor.fetchone()[0]
            conn.commit()
        except Exception as db_err:
            print(f"Error crítico escribiendo auditoría RAW: {db_err}")
            conn.rollback()
            continue # Salta para proteger el flujo continuo

        # Filtrado de Calidad de Datos
        if status != "VALID":
            print(f"⚠️ Evento del Offset {offset} interceptado y marcado como {status}. Motivo: {debug_reason}. Omitiendo predicción.")
            continue

        # Inferencia Consistente y Carga de Hechos
        try:
            # Mantener orden exacto de entrenamiento de variables predictoras
            ordered_features = [[
                float(payload["gdp"]),
                float(payload["family"]),
                float(payload["health"]),
                float(payload["freedom"]),
                float(payload["generosity"]),
                float(payload["corruption"])
            ]]
            
            # Ejecutar Predicción
            predicted_score = float(model.predict(ordered_features)[0])
            actual_score = float(payload["actual_happiness_score"])
            prediction_error = abs(actual_score - predicted_score)
            
            # Inserción Relacional Dimensional Integrada con Manejo de Conflictos 
            cursor.execute(
                """
                INSERT INTO dim_country (country_name) 
                VALUES (%s) 
                ON CONFLICT (country_name) DO UPDATE SET country_name=EXCLUDED.country_name 
                RETURNING country_id;
                """,
                (payload["country"],)
            )
            country_id = cursor.fetchone()[0]
            
            cursor.execute(
                """
                INSERT INTO dim_date (year) VALUES (%s) 
                ON CONFLICT (year) DO UPDATE SET year=EXCLUDED.year 
                RETURNING date_id;
                """,
                (int(payload["year"]),)
            )
            date_id = cursor.fetchone()[0]
            
            # Insertar Fila en la Tabla de Hechos final vinculada a su evento RAW de origen
            cursor.execute(
                """
                INSERT INTO fact_predictions (raw_event_id, country_id, date_id, actual_score, predicted_score, prediction_error)
                VALUES (%s, %s, %s, %s, %s, %s);
                """,
                (raw_event_id, country_id, date_id, actual_score, predicted_score, prediction_error)
            )
            conn.commit()
            print(f"✅ Procesado Exitosamente -> {payload['country']} ({payload['year']}) | MAE Local: {prediction_error:.4f}")
            
        except Exception as infer_error:
            print(f"❌ Error durante el cálculo matemático de predicción en offset {offset}: {infer_error}")
            
            # Limpiamos el estado fallido de la transacción actual para permitir nuevos comandos SQL
            conn.rollback() 
            
            try:
                cursor.execute(
                    "UPDATE raw_happiness_events SET processing_status = 'PREDICTION_ERROR' WHERE raw_event_id = %s;",
                    (raw_event_id,)
                )
                conn.commit()
            except Exception as update_err:
                print(f"No se pudo actualizar el estado de error en RAW: {update_err}")
                conn.rollback()

if __name__ == "__main__":
    run_consumer()