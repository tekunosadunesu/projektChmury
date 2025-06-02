import matplotlib.pyplot as plt
import pystac_client
import planetary_computer
import geopandas as gpd
import rioxarray
import streamlit as st
import leafmap.foliumap as leafmap
import sqlalchemy
from get_conn import get_connection_uri

# https://learn.microsoft.com/en-us/azure/postgresql/flexible-server/connect-python?tabs=cmd%2Cpasswordless
# granice wroclawia z bazy
# engine = sqlalchemy.create_engine("postgresql+psycopg2://dariusz:postgres@localhost:5432/TBD_wroc")
# gdf = gpd.read_postgis("select * from gran_dzielnice", con=engine, geom_col="geom")
# gdf = gdf.to_crs("EPSG:32633")

engine = sqlalchemy.create_engine(get_connection_uri())

# https://planetarycomputer.microsoft.com/docs/quickstarts/reading-stac/
# pobieranie danych z planetary computer
catalog = pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1",
    modifier=planetary_computer.sign_inplace,
)
time_range = "2024-04-01/2025-04-30"
bbox = [16.8, 51.04, 17.17, 51.21]
search = catalog.search(collections=["sentinel-2-l2a"], bbox=bbox, datetime=time_range)
items = search.item_collection()

selected_item = min(items, key=lambda item: item.properties["eo:cloud_cover"])

# wczytywanie danego pasma
def load_band(item, band_name, match=None):
    band = rioxarray.open_rasterio(item.assets[band_name].href, overview_level=1).squeeze()
    band = band.astype("float32") / 10000.0
    # band = band.rio.clip(gdf.geometry.values, gdf.crs)
    if match is not None:
        band = band.rio.reproject_match(match)
    return band

# obliczanie wskaznikow
def calc_index(index):
    if index == "NDVI":
        red = load_band(selected_item, "B04")
        nir = load_band(selected_item, "B08")
        ndvi = (nir - red) / (nir + red)
        return ndvi
    elif index == "NDII":
        swir = load_band(selected_item, "B11")
        nir = load_band(selected_item, "B08", match=swir)
        ndii = (nir - swir) / (nir + swir)
        return ndii
    elif index == "NDBI":
        swir = load_band(selected_item, "B11")
        nir = load_band(selected_item, "B08", match=swir)
        ndbi = (swir - nir) / (swir + nir)
        return ndbi
    elif index == "NDWI":
        green = load_band(selected_item, "B03")
        nir = load_band(selected_item, "B08")
        ndwi = (green - nir) / (green + nir)
        return ndwi

# STREAMLIT
st.title("Wizualizacja wskaźników")

index = st.selectbox("Wybierz wskaźnik", ["NDVI", "NDII", "NDBI", "NDWI"])
index_data = calc_index(index)

cmap = st.selectbox("Mapa kolorów", ["RdYlGn", "coolwarm", "RdGy", "CMRmap"])

st.subheader(f"Wskaźnik: {index}")

# https://learn.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python?tabs=managed-identity%2Croles-azure-portal%2Csign-in-azure-cli&pivots=blob-storage-quickstart-scratch#upload-blobs-to-a-container
# zapis danych
raster = f"tmp{index}{cmap}.tif"
index_data.rio.to_raster(raster)
# mapa
m = leafmap.Map(center=[51.1, 16.95], zoom=11)
m.add_raster(raster, layer_name=index, colormap=cmap)
m.to_streamlit()

fig, ax = plt.subplots(figsize=(8, 6))
im = ax.imshow(calc_index(index), cmap=cmap)
plt.colorbar(im, ax=ax, label=index)
ax.axis("off")
st.pyplot(fig)


