import time
import json
import os
import pandas as pd
from kafka import KafkaProducer

# Configuración del Broker local de Docker
BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'happiness-predictions'

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

def run_producer():
    print("Iniciando Kafka Producer...")
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=json_serializer
    )
    
    # Reutilizamos la lógica analizada para garantizar correspondencia exacta de tipos
    from sys import path
    path.append('../notebooks') # Permite importar lógica local si fuese necesario
    
    data_dir = "data/raw/"
    years = [2015, 2016, 2017, 2018, 2019]
    records_counter = 0

    for y in years:
        file_path = os.path.join(data_dir, f"{y}.csv")
        if not os.path.exists(file_path):
            continue
            
        # Mapeo rápido directo para la transmisión streaming línea por línea
        df = pd.read_csv(file_path)
        df.columns = df.columns.str.lower().str.strip().str.replace('.', ' ', regex=False)
        
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            
            try:
                # Obtener valores con llaves dinámicas compatibles con tus CSVs
                country = next((row_dict[k] for k in row_dict if 'country' in k), "Unknown")
                gdp = float(next((row_dict[k] for k in row_dict if 'gdp' in k or 'economy' in k), 0.0))
                family = float(next((row_dict[k] for k in row_dict if 'family' in k or 'social support' in k), 0.0))
                health = float(next((row_dict[k] for k in row_dict if 'health' in k or 'life' in k), 0.0))
                freedom = float(next((row_dict[k] for k in row_dict if 'freedom' in k), 0.0))
                generosity = float(next((row_dict[k] for k in row_dict if 'generosity' in k), 0.0))
                corruption = next((row_dict[k] for k in row_dict if 'corruption' in k or 'trust' in k), 0.0)
                corruption = float(corruption) if pd.notna(corruption) else 0.0
                actual_score = float(next((row_dict[k] for k in row_dict if 'score' in k), 0.0))
                
                payload = {
                    "country": str(country),
                    "year": int(y),
                    "gdp": gdp,
                    "family": family,
                    "health": health,
                    "freedom": freedom,
                    "generosity": generosity,
                    "corruption": corruption,
                    "actual_happiness_score": actual_score
                }
                
                # Inyectar datos con fallas artificiales para validar 
                records_counter += 1
                if records_counter % 30 == 0:
                    print("⚠️ Inyectando Registro Corrupto de Control: INVALID_SCHEMA")
                    payload.pop("gdp")  # Se elimina una columna requerida por el modelo
                elif records_counter % 45 == 0:
                    print("⚠️ Inyectando Registro Corrupto de Control: INVALID_VALUES")
                    payload["corruption"] = -5.5  # Valor imposible fuera de rangos válidos
                
                # Transmitir a Kafka
                producer.send(TOPIC_NAME, value=payload)
                time.sleep(0.3)  # Frecuencia de envío en tiempo real (300 ms)
                
            except Exception as e:
                print(f"Error parseando fila localmente en el productor: {e}")
                
    producer.flush()
    print("Transmisión de flujos históricos completada con éxito.")

if __name__ == "__main__":
    run_producer()