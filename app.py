import streamlit as st
import rioxarray
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
import io
import os
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import urllib.parse
import psycopg2

def get_connection_uri():
    dbhost = os.environ['DBHOST']
    dbname = os.environ['DBNAME']
    dbuser = os.environ['DBUSER']
    dbpassword = os.environ['DBPASSWORD']
    sslmode = os.environ['SSLMODE']

    db_uri = f"postgresql://{dbuser}:{dbpassword}@{dbhost}/{dbname}?sslmode={sslmode}"
    return db_uri

def get_blob_service_client():
    account_name = os.getenv("AZURE_STORAGE_ACCOUNT")
    account_url = f"https://{account_name}.blob.core.windows.net"
    credential = DefaultAzureCredential()
    return BlobServiceClient(account_url, credential)

def blob_read(blob_name):
    blob_service_client = get_blob_service_client()
    blob_client = blob_service_client.get_blob_client("indeksy", blob_name)

    stream = io.BytesIO()
    blob_client.download_blob().readinto(stream)
    stream.seek(0)
    return rioxarray.open_rasterio(stream)


st.title("Wizualizacja wskaźników spektralnych")

index = st.selectbox("Wybierz wskaźnik", ["NDVI", "NDII", "NDBI", "NDWI"])
cmap = st.selectbox("Wybierz mapę kolorów", ["RdYlGn", "coolwarm", "RdGy", "CMRmap"])

blob_name = f"{index}_{cmap}.tif"
raster = blob_read(blob_name).squeeze()

# Wyświetlenie rastra
fig, ax = plt.subplots(figsize=(8, 6))
im = ax.imshow(raster, cmap=cmap)
plt.colorbar(im, ax=ax, label=index)
ax.axis("off")
st.pyplot(fig)

# --- Obliczanie statystyk ---
array = raster.values
array = np.where(np.isnan(array), np.nan, array)

stats = {
    "index": index,
    "colormap": cmap,
    "min": float(np.nanmin(array)),
    "max": float(np.nanmax(array)),
    "mean": float(np.nanmean(array)),
    "std": float(np.nanstd(array)),
    "timestamp": datetime.utcnow()
}

with st.expander("📊 Statystyki wskaźnika"):
    df_display = pd.DataFrame([{
        "Wskaźnik": stats["index"],
        "Mapa kolorów": stats["colormap"],
        "Min": round(stats["min"], 4),
        "Max": round(stats["max"], 4),
        "Średnia": round(stats["mean"], 4),
        "Odchylenie std": round(stats["std"], 4),
        "Czas zapisu": stats["timestamp"].strftime("%Y-%m-%d %H:%M:%S UTC")
    }])
    st.dataframe(df_display, use_container_width=True)


# --- Zapis do bazy ---
try:
    conn_str = get_connection_uri()
    engine = create_engine(conn_str)

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS index_stats (
        id SERIAL PRIMARY KEY,
        index VARCHAR(50),
        colormap VARCHAR(50),
        min FLOAT,
        max FLOAT,
        mean FLOAT,
        std FLOAT,
        timestamp DATETIME
    );
    """

    with engine.begin() as conn:
        conn.execute(text(create_table_sql))

    df_stats = pd.DataFrame([stats])
    df_stats.to_sql("index_stats", engine, if_exists="append", index=False)

    st.success("Zapisano statystyki do bazy danych.")
except Exception as e:
    st.error(f"Błąd zapisu do bazy: {e}")
