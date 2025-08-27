# supabase_db.py — Supabase backend for your inventory app
# Works with tables: products, customers, suppliers, payments, stock_moves, users

import streamlit as st
import pandas as pd
from datetime import datetime
from supabase import create_client, Client

# ===================== Expected headers (column order) =====================
REQUIRED_TABS = {
    "Products":   ["id", "name", "material", "size", "unit", "opening_stock"],
    "Customers":  ["id", "name", "phone", "address"],
    "Suppliers":  ["id", "name", "phone", "address"],
    # supplier_id is used for purchases; customer_id for sales.
    "StockMoves": ["id", "ts", "kind", "product_id", "qty", "price_per_unit", "customer_id", "supplier_id", "notes"],
    # kind: opening_due | payment | advance
    "Payments":   ["id", "ts", "customer_id", "kind", "amount", "notes"],
    "Users":      ["id", "username", "password_hash", "salt"],
}

# Columns coerced to numeric on read
NUMERIC_COLUMNS = {
    "Products":   {"id", "opening_stock"},
    "Customers":  {"id"},
    "Suppliers":  {"id"},
    "StockMoves": {"id", "product_id", "qty", "price_per_unit", "customer_id", "supplier_id"},
    "Payments":   {"id", "customer_id", "amount"},
    "Users":      {"id"},
}

# Map UI "tab" -> Supabase table name
def _table(tab_name: str) -> str:
    return {
        "Products":   "products",
        "Customers":  "customers",
        "Suppliers":  "suppliers",
        "StockMoves": "stock_moves",
        "Payments":   "payments",
        "Users":      "users",
    }[tab_name]

@st.cache_resource(show_spinner=False)
def _client() -> Client:
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets.get("SUPABASE_SERVICE_ROLE_KEY") or st.secrets["SUPABASE_ANON_KEY"]
    cli = create_client(url, key)
    cli.postgrest.schema("public")
    return cli

def ensure_all_tabs():
    """Check tables exist. Stops the app with a visible error if any missing."""
    cli = _client()
    missing = []
    for tab in REQUIRED_TABS.keys():
        t = _table(tab)
        try:
            _ = cli.table(t).select("id").limit(1).execute()
        except Exception as e:
            msg = str(e).lower()
            if "does not exist" in msg or ("relation" in msg and "does not exist" in msg):
                missing.append(t)
            else:
                st.warning(f"⚠️ While checking `{t}`: {e}")
    if missing:
        st.error(
            "❌ Supabase tables not accessible: " + ", ".join(missing) +
            ". Create them (or fix schema/role) and refresh."
        )
        st.stop()

# ===================== Reads =====================
@st.cache_data(ttl=15, show_spinner=False)
def fetch_df(tab_name: str) -> pd.DataFrame:
    """
    Read a table into a DataFrame with stable column order and numeric coercions.
    If columns are missing in Supabase, they are added as NA to keep the UI safe.
    """
    cli = _client()
    t = _table(tab_name)

    # Default ordering per table
    default_order = {
        "Products":   ("name", True),
        "Customers":  ("name", True),
        "Suppliers":  ("name", True),
        "StockMoves": ("ts", True),   # ascending time
        "Payments":   ("ts", True),
        "Users":      ("username", True),
    }[tab_name]

    try:
        q = cli.table(t).select("*")
        col, asc = default_order
        q = q.order(col, desc=not asc)
        data = q.execute().data or []
    except Exception:
        return pd.DataFrame(columns=REQUIRED_TABS[tab_name])

    df = pd.DataFrame(data)
    if df.empty:
        return pd.DataFrame(columns=REQUIRED_TABS[tab_name])

    # Ensure all expected columns exist (even if null in DB)
    for col in REQUIRED_TABS[tab_name]:
        if col not in df.columns:
            df[col] = pd.NA

    # Coerce numerics
    for col in NUMERIC_COLUMNS.get(tab_name, set()):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Trim strings
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str).str.strip()

    # Keep column order stable
    return df[REQUIRED_TABS[tab_name]]

# ===================== Writes =====================
def _drop_absent_columns_for_insert(tab_name: str, rec: dict) -> dict:
    """Remove keys not present in the remote table (safety for schema drift)."""
    try:
        cols_present = set(fetch_df(tab_name).columns)
        return {k: v for k, v in rec.items() if k in cols_present}
    except Exception:
        return rec

def append_row(tab_name: str, row: list):
    """
    Insert one row using the tab's column order; returns the inserted dict (with id).
    Example: append_row("Products", [None, "Renite", "Tiles", "600x600", "box", 0])
    For identity PK tables, pass None for id (or omit).
    """
    cli = _client()
    t = _table(tab_name)
    cols = REQUIRED_TABS[tab_name]

    rec = {}
    for i, col in enumerate(cols):
        rec[col] = row[i] if i < len(row) else None

    # If id is None/blank, drop it so Postgres identity can fill it
    if "id" in rec and (rec["id"] in (None, "", pd.NA)):
        rec.pop("id")

    # Normalize timestamp columns
    if "ts" in rec and isinstance(rec.get("ts"), str):
        try:
            datetime.fromisoformat(rec["ts"].replace("Z", "+00:00"))
        except Exception:
            rec["ts"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    # Safety: drop keys that aren't in remote table
    rec = _drop_absent_columns_for_insert(tab_name, rec)

    res = cli.table(t).insert(rec).execute()
    st.cache_data.clear()  # invalidate cached reads after a write

    data = getattr(res, "data", None)
    if isinstance(data, list) and data:
        return data[0]
    return rec
