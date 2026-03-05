import streamlit as st
import pandas as pd
import requests
import io

DATA_URL = "https://github.com/cfvergaraortiz/dashboard-retiros/releases/download/v1.0/retiros_normalizado_sinR.parquet"

st.title("Test de carga")

try:
    st.write("Descargando archivo...")
    resp = requests.get(DATA_URL, timeout=180)
    st.write(f"Status HTTP: {resp.status_code}")
    st.write(f"Tamaño: {len(resp.content) / 1024 / 1024:.1f} MB")

    st.write("Leyendo Parquet...")
    df = pd.read_parquet(io.BytesIO(resp.content))
    st.write(f"✅ Cargado OK: {df.shape[0]:,} filas, {df.shape[1]} columnas")
    st.write("Columnas:", list(df.columns))
    st.dataframe(df.head(5))

except Exception as e:
    st.error(f"❌ Error: {e}")
    import traceback
    st.code(traceback.format_exc())
