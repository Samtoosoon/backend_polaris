# streamlit_app.py
import sqlite3
import pandas as pd
import streamlit as st
from datetime import datetime
import pytz
import os

# ---------- CONFIG ----------
DB_NAME = "polaris_data.db"
DATA_TABLE = "polaris_products"
OPS_TABLE = "operations"
TIMEZONE = "Asia/Kolkata"

# ---------- DB UTILITIES ----------
def get_connection():
    return sqlite3.connect(DB_NAME, check_same_thread=False)

def table_exists(conn, table_name):
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
    return cur.fetchone() is not None

def ensure_ops_table(conn):
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {OPS_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            action TEXT,
            target_table TEXT,
            target_id INTEGER,
            timestamp_local TEXT,
            timestamp_utc TEXT,
            metadata TEXT
        )
    """)
    conn.commit()

def log_operation(conn, action, target_table=None, target_id=None, metadata=None):
    utc_time = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    local_time = datetime.now(pytz.timezone(TIMEZONE)).isoformat(timespec="seconds")
    conn.execute(
        f"INSERT INTO {OPS_TABLE} (action, target_table, target_id, timestamp_local, timestamp_utc, metadata) VALUES (?, ?, ?, ?, ?, ?)",
        (action, target_table, target_id, local_time, utc_time, metadata)
    )
    conn.commit()
    return {"local": local_time, "utc": utc_time}

def load_csv_to_db(csv_file):
    """Read CSV (path or buffer), add an integer id column starting at 1, replace table."""
    df = pd.read_csv(csv_file)
    # clean column names (optional)
    df.columns = [c.strip() for c in df.columns]
    # ensure id column
    df = df.reset_index(drop=True)
    df.insert(0, "id", df.index + 1)
    conn = get_connection()
    # replace table
    df.to_sql(DATA_TABLE, conn, if_exists="replace", index=False)
    # create an index on id if not exists (sqlite will keep it as a column)
    try:
        conn.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{DATA_TABLE}_id ON {DATA_TABLE}(id);")
    except Exception:
        pass
    conn.commit()
    # ensure ops table exists
    ensure_ops_table(conn)
    conn.close()
    return len(df)

def get_table_data(conn, table_name, limit=500, where_clause=None):
    if not table_exists(conn, table_name):
        return pd.DataFrame()
    query = f"SELECT * FROM {table_name}"
    if where_clause:
        query += f" WHERE {where_clause}"
    query += f" ORDER BY id LIMIT {limit}"
    return pd.read_sql_query(query, conn)

def add_product(conn, product_dict):
    # find max id
    cur = conn.cursor()
    cur.execute(f"SELECT MAX(id) FROM {DATA_TABLE}")
    r = cur.fetchone()
    next_id = 1 if r is None or r[0] is None else r[0] + 1
    product_dict['id'] = next_id
    # prepare columns and values
    cols = ", ".join(product_dict.keys())
    placeholders = ", ".join(["?"] * len(product_dict))
    values = list(product_dict.values())
    cur.execute(f"INSERT INTO {DATA_TABLE} ({cols}) VALUES ({placeholders})", values)
    conn.commit()
    log_operation(conn, action="CREATE", target_table=DATA_TABLE, target_id=next_id, metadata=str(product_dict.get("Title","")))
    return next_id

def update_product(conn, product_id, update_fields: dict):
    set_clause = ", ".join([f"{k} = ?" for k in update_fields.keys()])
    values = list(update_fields.values()) + [product_id]
    conn.execute(f"UPDATE {DATA_TABLE} SET {set_clause} WHERE id = ?", values)
    conn.commit()
    log_operation(conn, action="UPDATE", target_table=DATA_TABLE, target_id=product_id, metadata=str(update_fields))
    return True

def delete_product(conn, product_id):
    conn.execute(f"DELETE FROM {DATA_TABLE} WHERE id = ?", (product_id,))
    conn.commit()
    log_operation(conn, action="DELETE", target_table=DATA_TABLE, target_id=product_id)
    return True

def get_operations(conn, limit=200):
    ensure_ops_table(conn)
    return pd.read_sql_query(f"SELECT * FROM {OPS_TABLE} ORDER BY id DESC LIMIT {limit}", conn)


# ---------- STREAMLIT UI ----------
st.set_page_config(page_title="Polaris Backend", layout="wide")
st.title("Polaris Backend")

st.sidebar.header("Data Source")
uploaded_file = st.sidebar.file_uploader("Upload CSV (optional, default: polaris.csv)", type=["csv"])
use_local = False
if uploaded_file is None and os.path.exists("polaris.csv"):
    # auto-load local file if present
    uploaded_file = "polaris.csv"
    use_local = True

if uploaded_file:
    try:
        rows = load_csv_to_db(uploaded_file)
        st.sidebar.success(f"Loaded {rows} rows into '{DATA_TABLE}'.")
        if use_local:
            st.sidebar.info("Loaded from local polaris.csv")
    except Exception as e:
        st.sidebar.error(f"Failed to load CSV: {e}")

# DB connection
conn = get_connection()
ensure_ops_table(conn)

# ---- Top controls: search / filter ----
st.header("Products")
search_col1, search_col2, search_col3 = st.columns([3,2,1])
with search_col1:
    q = st.text_input("Search Title (substring)")
with search_col2:
    vis_filter = st.selectbox("PredictedVisibility", options=["All","High","Medium","Low"], index=0)
with search_col3:
    refresh = st.button("Refresh")

where_clauses = []
if q:
    # basic escape for single quote
    q_esc = q.replace("'", "''")
    where_clauses.append(f"Title LIKE '%{q_esc}%'")
if vis_filter != "All":
    where_clauses.append(f"PredictedVisibility = '{vis_filter}'")

where_clause = " AND ".join(where_clauses) if where_clauses else None
products_df = get_table_data(conn, DATA_TABLE, limit=1000, where_clause=where_clause)

# show dataframe with selectable rows
if products_df.empty:
    st.info("No products found. Load CSV in sidebar to initialize data.")
else:
    st.dataframe(products_df)

# ---- CRUD: Add new product ----
st.markdown("---")
st.subheader("Add a new product")
with st.form("add_product_form", clear_on_submit=True):
    title = st.text_input("Title")
    description = st.text_area("Description")
    price_stable = st.number_input("PriceStable", min_value=0, max_value=1, value=1, step=1)
    in_stock = st.number_input("InStock", min_value=0, max_value=1, value=1, step=1)
    seller_pin = st.text_input("SellerPincode")
    customer_pin = st.text_input("CustomerPincode")
    shipping_days = st.number_input("ShippingDays", min_value=0, value=3, step=1)
    predicted_visibility = st.selectbox("PredictedVisibility", options=["High","Medium","Low"], index=1)
    submitted = st.form_submit_button("Add product")
    if submitted:
        try:
            prod = {
                "Title": title,
                "Description": description,
                "PriceStable": price_stable,
                "InStock": in_stock,
                "SellerPincode": seller_pin,
                "CustomerPincode": customer_pin,
                "ShippingDays": shipping_days,
                "PredictedVisibility": predicted_visibility
            }
            new_id = add_product(conn, prod)
            st.success(f"Product added with id {new_id}")
            st.experimental_rerun()
        except Exception as e:
            st.error(f"Failed to add product: {e}")

# ---- CRUD: Select a product to Edit / Delete ----
st.markdown("---")
st.subheader("Edit / Delete product")
if products_df.empty:
    st.info("No products to edit.")
else:
    ids = products_df["id"].tolist()
    sel_id = st.selectbox("Select product id", options=ids)
    sel_row = products_df[products_df["id"] == int(sel_id)].iloc[0].to_dict()

    with st.form("edit_product_form"):
        e_title = st.text_input("Title", value=sel_row.get("Title",""))
        e_description = st.text_area("Description", value=sel_row.get("Description",""))
        e_price_stable = st.number_input("PriceStable", min_value=0, max_value=1, value=int(sel_row.get("PriceStable",1)), step=1)
        e_in_stock = st.number_input("InStock", min_value=0, max_value=1, value=int(sel_row.get("InStock",1)), step=1)
        e_seller_pin = st.text_input("SellerPincode", value=str(sel_row.get("SellerPincode","")))
        e_customer_pin = st.text_input("CustomerPincode", value=str(sel_row.get("CustomerPincode","")))
        e_shipping_days = st.number_input("ShippingDays", min_value=0, value=int(sel_row.get("ShippingDays",0)), step=1)
        e_pred_vis = st.selectbox("PredictedVisibility", options=["High","Medium","Low"], index=["High","Medium","Low"].index(sel_row.get("PredictedVisibility","Medium")))
        save_btn = st.form_submit_button("Save changes")
        del_btn = st.form_submit_button("Delete product")

        if save_btn:
            try:
                updates = {
                    "Title": e_title,
                    "Description": e_description,
                    "PriceStable": e_price_stable,
                    "InStock": e_in_stock,
                    "SellerPincode": e_seller_pin,
                    "CustomerPincode": e_customer_pin,
                    "ShippingDays": e_shipping_days,
                    "PredictedVisibility": e_pred_vis
                }
                update_product(conn, int(sel_id), updates)
                st.success("Product updated.")
                st.experimental_rerun()
            except Exception as e:
                st.error(f"Update failed: {e}")

        if del_btn:
            try:
                delete_product(conn, int(sel_id))
                st.success("Product deleted.")
                st.experimental_rerun()
            except Exception as e:
                st.error(f"Delete failed: {e}")

# ---- Operations Log ----
st.markdown("---")
st.header("Operations Log")
ops_df = get_operations(conn, limit=200)
st.dataframe(ops_df)

st.markdown("---")
st.info("Polaris backend stores products in SQLite and logs every CRUD operation with timestamps.")
