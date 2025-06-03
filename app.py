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

def get_connection_uri():

    # Read URI parameters from the environment
    dbhost = os.environ['DBHOST']
    dbname = os.environ['DBNAME']
    dbuser = urllib.parse.quote(os.environ['DBUSER'])
    sslmode = os.environ['SSLMODE']


    # Use passwordless authentication via DefaultAzureCredential.
    # IMPORTANT! This code is for demonstration purposes only. DefaultAzureCredential() is invoked on every call.
    # In practice, it's better to persist the credential across calls and reuse it so you can take advantage of token
    # caching and minimize round trips to the identity provider. To learn more, see:
    # https://github.com/Azure/azure-sdk-for-python/blob/main/sdk/identity/azure-identity/TOKEN_CACHING.md
    credential = DefaultAzureCredential()

    # Call get_token() to get a token from Microsft Entra ID and add it as the password in the URI.
    # Note the requested scope parameter in the call to get_token, "https://ossrdbms-aad.database.windows.net/.default".
    password = credential.get_token("https://ossrdbms-aad.database.windows.net/.default").token

    db_uri = f"postgresql://{dbuser}:{password}@{dbhost}/{dbname}?sslmode={sslmode}"
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

st.subheader("Statystyki wskaźnika")
st.write(stats)

# --- Zapis do bazy ---
try:
    conn_str = get_connection_uri()
    engine = create_engine(conn_str)

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS index_stats (
        id INT IDENTITY(1,1) PRIMARY KEY,
        [index] NVARCHAR(10),
        colormap NVARCHAR(50),
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
