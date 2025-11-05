import sqlite3
import pandas as pd
import streamlit as st
from datetime import datetime
import pytz
import os

# ---------- CONFIG ----------
DB_NAME = "amazon_data.db"
DATA_TABLE = "amazon_products"
OPS_TABLE = "operations"
TIMEZONE = "Asia/Kolkata"  # change if needed

# ---------- DB UTILITIES ----------
def get_connection():
    return sqlite3.connect(DB_NAME, check_same_thread=False)

def table_exists(conn, table_name):
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
    return cur.fetchone() is not None

def load_csv_to_db(csv_file):
    df = pd.read_csv(csv_file)
    conn = get_connection()
    df.to_sql(DATA_TABLE, conn, if_exists="replace", index=False)
    conn.commit()
    conn.close()
    return len(df)

def ensure_ops_table(conn):
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {OPS_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            action TEXT,
            timestamp_local TEXT,
            timestamp_utc TEXT
        )
    """)
    conn.commit()

def log_operation(conn, action="trigger"):
    utc_time = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    local_time = datetime.now(pytz.timezone(TIMEZONE)).isoformat(timespec="seconds")
    conn.execute(
        f"INSERT INTO {OPS_TABLE} (action, timestamp_local, timestamp_utc) VALUES (?, ?, ?)", 
        (action, local_time, utc_time)
    )
    conn.commit()
    return local_time

def get_table_data(conn, table_name, limit=50):
    if not table_exists(conn, table_name):
        return pd.DataFrame()
    return pd.read_sql_query(f"SELECT * FROM {table_name} LIMIT {limit}", conn)

def get_operations(conn, limit=50):
    ensure_ops_table(conn)
    return pd.read_sql_query(f"SELECT * FROM {OPS_TABLE} ORDER BY id DESC LIMIT {limit}", conn)

# ---------- STREAMLIT UI ----------
st.set_page_config(page_title="Amazon CSV → SQLite Backend", layout="wide")

st.title("Amazon CSV to SQLite Backend")

# Sidebar
st.sidebar.header("Load or Initialize Database")

# Use uploaded file or pre-specified one
uploaded_file = st.sidebar.file_uploader("Upload your CSV file (default: polaris.csv)", type=["csv"])

# If no file is uploaded, use local polaris.csv
if uploaded_file is None and os.path.exists("polaris.csv"):
    uploaded_file = "polaris.csv"

if uploaded_file:
    rows = load_csv_to_db(uploaded_file)
    st.sidebar.success(f"Loaded {rows} rows into '{DATA_TABLE}' table.")

# Database connection
conn = get_connection()
ensure_ops_table(conn)

# Main UI
st.header("Product Data Preview")
df = get_table_data(conn, DATA_TABLE, limit=200)
if df.empty:
    st.warning("No data in the database yet. Upload your CSV file from the sidebar.")
else:
    st.dataframe(df)

st.markdown("---")
st.header("Trigger Operation")

if st.button("Trigger Backend Operation"):
    local_time = log_operation(conn, action="manual_trigger")
    st.success(f"Operation triggered successfully at {local_time}")

st.markdown("---")
st.header("Operations Log")
ops = get_operations(conn, limit=100)
st.dataframe(ops)

st.markdown("---")
st.info("Upload your Amazon CSV (or use polaris.csv) → stored in SQLite → trigger logs timestamped operations.")
