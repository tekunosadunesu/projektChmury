import streamlit as st
import rioxarray
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
import io
import os
import matplotlib.pyplot as plt


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

# Nazwa pliku wynikowego
blob_name = f"{index}_{cmap}.tif"

# Wersja bez wywołania Azure Function
st.info("Wczytywanie danych z chmury Azure...")

# Wczytanie i wizualizacja
raster = blob_read(blob_name)

fig, ax = plt.subplots(figsize=(8, 6))
im = ax.imshow(raster.squeeze(), cmap=cmap)
plt.colorbar(im, ax=ax, label=index)
ax.axis("off")
st.pyplot(fig)
