# Taller 3: Streaming ETL con Apache Kafka y Machine Learning
**Estudiante:** Julian D. Cuellar  
**Curso:** ETL (G01)  
**Programa:** Ingeniería de Datos e Inteligencia Artificial  
**Universidad Autónoma de Occidente (UAO)**

---

## 1. Descripción del Proyecto
En este tercer taller, diseñé e implementé un pipeline de Streaming ETL de extremo a extremo para procesar datos en tiempo real del Reporte Mundial de Felicidad (2015-2019). Mi objetivo fue simular la llegada continua de métricas socioeconómicas usando Apache Kafka, validar la calidad de esos datos "al vuelo", hacer predicciones en tiempo real con un modelo de Machine Learning que entrené previamente, y finalmente guardar todo en una base de datos PostgreSQL usando un modelo dimensional.

---

## 2. Arquitectura del Sistema
Para mantener el diseño limpio y desacoplado, decidí dividir el sistema en dos grandes fases:

1. **Proceso Offline (Batch):**
   * **Análisis y Limpieza:** Primero, analicé los CSVs históricos para entender cómo venían los datos cada año.
   * **Entrenamiento:** Construí y evalué un modelo de regresión (`RandomForestRegressor`) para predecir el puntaje de felicidad.
   * **Serialización:** Exporté mi modelo entrenado a un archivo (`models/model.pkl`) usando `joblib` para poder consumirlo después.

2. **Proceso Online (Streaming):**
   * **Kafka Producer:** Programé un script que lee los CSVs, arregla los problemas de formato de cada año y envía los datos como mensajes JSON a Kafka.
   * **Kafka Broker:** Usé contenedores de Docker para levantar el broker y gestionar el tópico `happiness-predictions`.
   * **Kafka Consumer:** Este es el corazón de mi streaming. Atrapa los mensajes, revisa que no traigan errores, ejecuta la predicción matemática con el modelo cargado y guarda los resultados limpios en mi base de datos.

---

## 3. Decisiones de Limpieza de Datos
Al explorar los datasets originales, noté que los nombres de las columnas eran un desastre entre 2015 y 2019 (por ejemplo, el PIB aparecía como `Economy (GDP per Capita)`, `Economy..GDP.per.Capita.` o simplemente `GDP per capita`). Para arreglar esto, apliqué estas reglas en mi código:

* **Normalización de texto:** Pasé todos los nombres de columnas a minúsculas, quité los espacios extra y cambié los puntos por espacios.
* **Mapeo dinámico:** En lugar de hacer reglas estáticas, programé en el productor una búsqueda por palabras clave (como buscar `'gdp'`, `'family'` o `'health'`) para que el código detecte y unifique las variables automáticamente, sin importar de qué año venga el archivo.
* **Conversión de tipos:** Obligué a que todas las métricas pasaran a tipo `float` y el año a `int` para evitar que el consumidor o la base de datos fallaran por problemas de tipado.

---

## 4. Feature Engineering y Modelo Predictivo
Para que el modelo de Machine Learning no fallara en producción, definí un orden estricto de las variables de entrada. Las características que elegí fueron:

1. `gdp` (Producto Interno Bruto per cápita)
2. `family` (Soporte social)
3. `health` (Esperanza de vida saludable)
4. `freedom` (Libertad de elección)
5. `generosity` (Generosidad)
6. `corruption` (Percepción de corrupción)

**Mis resultados en la evaluación del modelo (Test Set - 30%):**
* **Algoritmo:** RandomForestRegressor (con 100 estimadores)
* **Error Absoluto Medio (MAE):** 0.4071  
* **Raíz del Error Cuadrático Medio (RMSE):** 0.5235  
* **Coeficiente de Determinación (R² Score):** 0.7805  

---

## 5. Pipeline de Kafka y Resiliencia (Calidad de Datos)
Una parte clave de este taller era asegurar que el pipeline no se cayera si llegaban datos malos. Para probarlo, hice lo siguiente:

### Inyección de Fallas a Propósito
En mi script del productor, programé la inyección de errores para validar mi arquitectura:
* **Cada 30 registros:** Le quito la columna `gdp` al JSON para simular un esquema incompleto (`INVALID_SCHEMA`).
* **Cada 45 registros:** Le mando un valor imposible de `-5.5` a la corrupción para simular un dato fuera de rango (`INVALID_VALUES`).

### Cómo atrapé los errores en el Consumidor
Programé el consumidor para que evalúe cada registro antes de pasarlo al modelo. Si detecto un `INVALID_SCHEMA` o un `INVALID_VALUES`, guardo el error en la tabla de auditoría para que quede el registro, lanzo una advertencia en la consola y simplemente salto al siguiente mensaje sin detener el programa.

Además, le agregué un manejo de transacciones a la base de datos (`conn.rollback()`). Si por algún motivo el cálculo matemático o la inserción falla, limpio la transacción en PostgreSQL y marco el registro como `PREDICTION_ERROR` en mi tabla RAW.

---

## 6. Modelo Dimensional de la Base de Datos
Diseñé mi base de datos en PostgreSQL usando un esquema en estrella, enfocado en que no se me duplicaran los datos si corría el script varias veces (idempotencia):

* **`raw_happiness_events`:** Mi tabla de auditoría. Aquí guardo absolutamente todo lo que llega de Kafka (el offset, el JSON crudo y si era válido o venía con errores).
* **`dim_country`:** Mi dimensión de países. Le puse una regla `UNIQUE` y un `ON CONFLICT DO UPDATE` para que cada país se guarde una sola vez.
* **`dim_date`:** Mi dimensión de tiempo (años), manejada con la misma lógica para evitar duplicados.
* **`fact_predictions`:** La tabla de hechos principal. Aquí cruzo las llaves de mis dimensiones y guardo el puntaje real, el que predijo mi modelo y el error que hubo entre ambos.

---

## 7. Instrucciones de Ejecución

Para correr todo mi proyecto desde cero, debes seguir estos pasos en orden:

### Paso 1: Levantar los contenedores de Docker
Inicia Kafka, Zookeeper y PostgreSQL en segundo plano ejecutando esto en la raíz del proyecto:

    docker-compose up -d

### Paso 2: Crear la Base de Datos
Conéctate al PostgreSQL de Docker (`localhost:5432`, Base de datos: `happiness_db`) y corre mi script de tablas:

    sql/create_tables.sql

### Paso 3: Entrenar el Modelo
Genera el archivo `.pkl` corriendo el script de entrenamiento:

    python notebooks/model_training.py

### Paso 4: Encender el Consumidor
Abre una terminal y pon a correr al consumidor para que se quede esperando los datos:

    python kafka/consumer.py

### Paso 5: Disparar el Productor
Abre otra terminal y ejecuta el productor para empezar a transmitir el flujo de datos:

    python kafka/producer.py

---

## 8. Dashboard y KPIs
Una vez que mi tabla de hechos se llenó, conecté mi herramienta de BI para graficar el comportamiento del sistema usando las consultas de mi archivo `sql/kpis.sql`. 

En el dashboard analicé:
1. **El Error Promedio (MAE Global):** Para ver qué tanto se está equivocando mi modelo en tiempo real.
2. **Los Filtros de Calidad:** Una gráfica para demostrar cuántos registros procesé con éxito y cuántos datos corruptos logró atajar mi consumidor.
3. **Países más felices:** La comparación geográfica entre el puntaje real y el que yo predije.
4. **Tendencia en el tiempo:** Cómo ha ido cambiando la felicidad mundial entre 2015 y 2019.