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

def get_connection():

    dbhost = os.environ['DBHOST']
    dbname = os.environ['DBNAME']
    dbuser = os.environ['DBUSER']
    sslmode = os.environ['SSLMODE']
    password = os.environ['DBPASSWORD']

    conn = psycopg2.connect(
        host=dbhost,
        dbname=dbname,
        user=dbuser,
        password=password,
        sslmode=sslmode
    )
    return conn

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


def stats_sidebar():
    with st.sidebar.expander("Odczytane statystyki z bazy"):
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT index, colormap, min, max, mean, std, timestamp FROM index_stats ORDER BY timestamp DESC LIMIT 10;")
            rows = cursor.fetchall()
            cursor.close()
            conn.close()

            if rows:
                df_db = pd.DataFrame(rows,
                                     columns=["Wskaźnik", "Mapa kolorów", "Min", "Max", "Średnia", "Odchylenie std",
                                              "Czas zapisu"])
                df_db["Czas zapisu"] = df_db["Czas zapisu"].dt.strftime("%Y-%m-%d %H:%M:%S UTC") if hasattr(
                    df_db["Czas zapisu"], "dt") else df_db["Czas zapisu"]
                st.dataframe(df_db, use_container_width=True)
            else:
                st.write("Brak danych w bazie.")
        except Exception as e:
            st.error(f"Błąd odczytu z bazy: {e}")


def stats_dist_sidebar(array, index_name):
    with st.sidebar.expander("Statystyki rozkładu wskaźnika"):
        show_hist = st.checkbox("Pokaż histogram z rozkładem normalnym")
        show_min_max_mean = st.checkbox("Pokaż wykres Min, Max, Średnia, Ochylenie std")

        if st.button("Pokaż wybrane statystyki"):
            clean_array = array[np.isfinite(array)]

            if len(clean_array) == 0:
                st.write("Brak danych do analizy.")
                return

            mean_val = np.mean(clean_array)
            std_val = np.std(clean_array)
            min_val = np.min(clean_array)
            max_val = np.max(clean_array)

            if show_hist:
                st.write(f"**Histogram i rozkład normalny dla {index_name}**")
                fig, ax = plt.subplots(figsize=(6,4))
                count, bins, ignored = ax.hist(clean_array, bins=50, density=True, alpha=0.6, color='g', label="Histogram")

                xmin, xmax = bins[0], bins[-1]
                x = np.linspace(xmin, xmax, 100)

                def standard_dev(x, mean, std):
                    first = 1 / (std * np.sqrt(2 * np.pi))
                    second = np.exp(- ((x - mean) ** 2) / (2 * std ** 2))
                    return first * second

                p = standard_dev(x, mean_val, std_val)
                ax.plot(x, p, 'k', linewidth=2, label='Rozkład normalny')

                ax.set_title(f"Histogram i rozkład normalny: {index_name}")
                ax.legend()
                st.pyplot(fig)

            if show_min_max_mean:
                st.write(f"Wartości Min, Max, Średnia z odchyleniem standardowym dla {index_name}")
                fig, ax = plt.subplots(figsize=(6, 3))

                bars = ax.bar(['Min', 'Średnia', 'Max'], [min_val, mean_val, max_val], color=['blue', 'orange', 'green'])

                ax.errorbar(1, mean_val, yerr=std_val, fmt='none', ecolor='red', capsize=5, label='Odchylenie standardowe')

                ax.set_title(f"Min, Średnia, Max i Odchylenie standardowe: {index_name}")
                ax.legend()
                st.pyplot(fig)

            if not show_hist and not show_min_max_mean:
                st.write("Zaznacz co chcesz zobaczyć i kliknij przycisk.")

def stats_expander():
    with st.expander("Statystyki wskaźnika"):
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


st.title("Wizualizacja wskaźników spektralnych")

stats_sidebar()

index = st.selectbox("Wybierz wskaźnik", ["NDVI", "NDII", "NDBI", "NDWI"])
cmap = st.selectbox("Wybierz mapę kolorów", ["RdYlGn", "coolwarm", "RdGy", "CMRmap"])

blob_name = f"{index}_{cmap}.tif"
raster = blob_read(blob_name).squeeze()

# wyswietlanie
fig, ax = plt.subplots(figsize=(8, 6))
im = ax.imshow(raster, cmap=cmap)
plt.colorbar(im, ax=ax, label=index)
ax.axis("off")
st.pyplot(fig)

# obliczanie statystyk
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

stats_expander()

array = raster.values
array = np.where(np.isnan(array), np.nan, array)
stats_dist_sidebar(array, index)

# zapis do bazyu
if st.button("Zapisz statystyki do bazy"):
    try:
        conn = get_connection()
        cursor = conn.cursor()

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS index_stats (
            id SERIAL PRIMARY KEY,
            index VARCHAR(50),
            colormap VARCHAR(50),
            min FLOAT,
            max FLOAT,
            mean FLOAT,
            std FLOAT,
            timestamp TIMESTAMP
        );
        """
        cursor.execute(create_table_sql)
        conn.commit()

        cursor.execute("""
            SELECT 1 FROM index_stats WHERE index = %s AND colormap = %s LIMIT 1;
        """, (stats["index"], stats["colormap"]))
        exists = cursor.fetchone()

        if exists:
            st.info(f"Statystyki dla wskaźnika {stats['index']} z mapą kolorów {stats['colormap']} już istnieją w bazie i nie zostaną nadpisane.")
        else:
            insert_sql = """
            INSERT INTO index_stats (index, colormap, min, max, mean, std, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_sql, (
                stats["index"],
                stats["colormap"],
                stats["min"],
                stats["max"],
                stats["mean"],
                stats["std"],
                stats["timestamp"]
            ))
            conn.commit()
            st.success("Zapisano statystyki do bazy danych.")

        cursor.close()
        conn.close()

    except Exception as e:
        st.error(f"Błąd zapisu do bazy: {e}")
