import os

import pandas as pd
import psycopg2
import pydeck as pdk
import streamlit as st

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "flights")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "flights")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "flights")

LONDON_LAT = 51.4700
LONDON_LON = -0.4543


@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def load_current_flights():
    conn = get_connection()
    query = """
        SELECT icao24, callsign, origin_country, longitude, latitude,
               baro_altitude_m, velocity_ms, heading_deg, on_ground, updated_at
        FROM current_flights
        WHERE longitude IS NOT NULL AND latitude IS NOT NULL
        ORDER BY updated_at DESC
    """
    return pd.read_sql(query, conn)


st.set_page_config(page_title="Live Flight Tracker — London", layout="wide")
st.title("✈️ Live Flight Tracker — London")
st.caption("OpenSky Network → Kafka → Postgres → Streamlit")

df = load_current_flights()

if df.empty:
    st.warning("No flight data yet. Is the producer/consumer pipeline running?")
    st.stop()

col1, col2, col3 = st.columns(3)
col1.metric("Aircraft tracked", len(df))
col2.metric("Airborne", int((~df["on_ground"].fillna(False)).sum()))
col3.metric("On ground", int(df["on_ground"].fillna(False).sum()))

df["altitude_ft"] = (df["baro_altitude_m"].fillna(0) * 3.281).round(0)
df["speed_kts"] = (df["velocity_ms"].fillna(0) * 1.944).round(0)

layer = pdk.Layer(
    "ScatterplotLayer",
    data=df,
    get_position=["longitude", "latitude"],
    get_radius=200,
    get_fill_color=[255, 90, 0, 180],
    pickable=True,
)

view_state = pdk.ViewState(
    latitude=LONDON_LAT,
    longitude=LONDON_LON,
    zoom=8,
    pitch=0,
)

tooltip = {
    "html": "<b>{callsign}</b><br/>"
            "Altitude: {altitude_ft} ft<br/>"
            "Speed: {speed_kts} kts<br/>"
            "Origin: {origin_country}",
    "style": {"backgroundColor": "steelblue", "color": "white"},
}

st.pydeck_chart(pdk.Deck(
    layers=[layer],
    initial_view_state=view_state,
    tooltip=tooltip,
    map_style=None,
))
