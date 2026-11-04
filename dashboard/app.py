import os

import pandas as pd
import psycopg2
import pydeck as pdk
import streamlit as st

LONDON_LAT = 51.4700
LONDON_LON = -0.4543
REFRESH_SECONDS = 30
STALE_AFTER_MINUTES = 5


def get_setting(name, default=None):
    """Read a setting from Streamlit secrets (Streamlit Community Cloud) or,
    failing that, from an environment variable (Docker Compose / local)."""
    try:
        if name in st.secrets:
            return st.secrets[name]
    except Exception:
        pass  # no secrets.toml present
    return os.environ.get(name, default)


def connect():
    database_url = get_setting("DATABASE_URL")
    if database_url:
        return psycopg2.connect(database_url, connect_timeout=10)
    return psycopg2.connect(
        host=get_setting("POSTGRES_HOST", "localhost"),
        port=get_setting("POSTGRES_PORT", "5432"),
        dbname=get_setting("POSTGRES_DB", "flights"),
        user=get_setting("POSTGRES_USER", "flights"),
        password=get_setting("POSTGRES_PASSWORD", "flights"),
        connect_timeout=10,
    )


@st.cache_resource
def get_connection():
    conn = connect()
    conn.autocommit = True
    return conn


QUERY = """
    SELECT icao24, callsign, origin_country, longitude, latitude,
           baro_altitude_m, velocity_ms, heading_deg, on_ground, updated_at
    FROM current_flights
    WHERE longitude IS NOT NULL AND latitude IS NOT NULL
      AND updated_at > now() - make_interval(mins => %s)
    ORDER BY updated_at DESC
"""


def run_query():
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(QUERY, (STALE_AFTER_MINUTES,))
        columns = [c.name for c in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=columns)


def load_current_flights():
    """Query current flights, reconnecting once if the cached connection has
    gone away (hosted databases like Neon close idle connections)."""
    try:
        return run_query()
    except (psycopg2.OperationalError, psycopg2.InterfaceError):
        get_connection.clear()
        return run_query()


st.set_page_config(page_title="Live Flight Tracker — London", layout="wide")
st.title("✈️ Live Flight Tracker — London")
st.caption("OpenSky Network → Kafka → Postgres → Streamlit")


@st.fragment(run_every=REFRESH_SECONDS)
def live_view():
    try:
        df = load_current_flights()
    except psycopg2.Error as exc:
        st.error(f"Couldn't reach the database: {exc}")
        return

    if df.empty:
        st.info(
            "No aircraft seen in the last few minutes. The ingestion pipeline "
            "may be offline right now. Check back later."
        )
        return

    on_ground = df["on_ground"].fillna(False).astype(bool)
    col1, col2, col3 = st.columns(3)
    col1.metric("Aircraft tracked", len(df))
    col2.metric("Airborne", int((~on_ground).sum()))
    col3.metric("On ground", int(on_ground.sum()))

    df["altitude_ft"] = (df["baro_altitude_m"].astype(float).fillna(0) * 3.281).round(0)
    df["speed_kts"] = (df["velocity_ms"].astype(float).fillna(0) * 1.944).round(0)
    df["callsign"] = df["callsign"].fillna(df["icao24"])

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=df,
        get_position=["longitude", "latitude"],
        get_radius=200,
        get_fill_color=[255, 90, 0, 180],
        pickable=True,
    )

    view_state = pdk.ViewState(latitude=LONDON_LAT, longitude=LONDON_LON, zoom=8, pitch=0)

    tooltip = {
        "html": "<b>{callsign}</b><br/>"
        "Altitude: {altitude_ft} ft<br/>"
        "Speed: {speed_kts} kts<br/>"
        "Origin: {origin_country}",
        "style": {"backgroundColor": "steelblue", "color": "white"},
    }

    st.pydeck_chart(
        pdk.Deck(layers=[layer], initial_view_state=view_state, tooltip=tooltip, map_style=None)
    )

    st.dataframe(
        df[["callsign", "icao24", "origin_country", "altitude_ft", "speed_kts", "heading_deg", "on_ground", "updated_at"]],
        hide_index=True,
        use_container_width=True,
        column_config={
            "callsign": "Callsign",
            "icao24": "ICAO24",
            "origin_country": "Origin",
            "altitude_ft": st.column_config.NumberColumn("Altitude (ft)", format="%d"),
            "speed_kts": st.column_config.NumberColumn("Speed (kts)", format="%d"),
            "heading_deg": st.column_config.NumberColumn("Heading (°)", format="%.0f"),
            "on_ground": "On ground",
            "updated_at": st.column_config.DatetimeColumn("Last update", format="HH:mm:ss"),
        },
    )
    st.caption(f"Refreshes every {REFRESH_SECONDS}s. Showing aircraft seen in the last {STALE_AFTER_MINUTES} minutes.")


live_view()
