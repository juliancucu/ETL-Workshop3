import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import joblib
import os

def load_and_harmonize(file_path, year):
    df = pd.read_csv(file_path)
    df.columns = df.columns.str.lower().str.strip().str.replace('.', ' ', regex=False).str.replace('  ', ' ', regex=False)
    harmonized = pd.DataFrame()
    
    country_col = [c for c in df.columns if 'country' in c]
    harmonized['country'] = df[country_col[0]] if country_col else "Unknown"
    harmonized['year'] = int(year)
    
    gdp_col = [c for c in df.columns if 'gdp' in c or 'economy' in c]
    harmonized['gdp'] = df[gdp_col[0]].astype(float)
    
    family_col = [c for c in df.columns if 'family' in c or 'social support' in c]
    harmonized['family'] = df[family_col[0]].astype(float)
    
    health_col = [c for c in df.columns if 'health' in c or 'life' in c]
    harmonized['health'] = df[health_col[0]].astype(float)
    
    freedom_col = [c for c in df.columns if 'freedom' in c]
    harmonized['freedom'] = df[freedom_col[0]].astype(float)
    
    generosity_col = [c for c in df.columns if 'generosity' in c]
    harmonized['generosity'] = df[generosity_col[0]].astype(float)
    
    corruption_col = [c for c in df.columns if 'corruption' in c or 'trust' in c]
    harmonized['corruption'] = df[corruption_col[0]].fillna(0.0).astype(float)
    
    score_col = [c for c in df.columns if 'score' in c or 'happiness score' in c]
    harmonized['actual_happiness_score'] = df[score_col[0]].astype(float)
    
    return harmonized

# Unificar los datasets históricos
data_dir = "data/raw/" # Ruta ajustada
years = [2015, 2016, 2017, 2018, 2019]
all_data = []

print("Leyendo archivos CSV...")
for y in years:
    path = os.path.join(data_dir, f"{y}.csv")
    if os.path.exists(path):
        all_data.append(load_and_harmonize(path, y))
    else:
        print(f"⚠️ Archivo no encontrado: {path}")

unified_df = pd.concat(all_data, ignore_index=True).dropna()

# Entrenar el modelo
FEATURES = ['gdp', 'family', 'health', 'freedom', 'generosity', 'corruption']
X = unified_df[FEATURES]
y = unified_df['actual_happiness_score']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.30, random_state=42)

print("Entrenando el modelo de Machine Learning...")
model = RandomForestRegressor(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# Evaluar Métricas
y_pred = model.predict(X_test)
print(f"--- Evaluación del Modelo ---")
print(f"MAE: {mean_absolute_error(y_test, y_pred):.4f}")
print(f"RMSE: {np.sqrt(mean_squared_error(y_test, y_pred)):.4f}")
print(f"R2 Score: {r2_score(y_test, y_pred):.4f}")

# Guardar Modelo
os.makedirs("models", exist_ok=True)
joblib.dump(model, "models/model.pkl")
print("✅ Modelo guardado exitosamente en models/model.pkl")