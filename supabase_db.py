# supabase_db.py — Supabase backend for your inventory app
# Keeps the same interfaces your app expects: ensure_all_tabs, fetch_df, append_row, etc.

import streamlit as st
import pandas as pd
from datetime import datetime
from supabase import create_client, Client

# ---- Expected headers (column order) — keep identical to your app ----
REQUIRED_TABS = {
    "Products":   ["id", "name", "material", "size", "unit", "opening_stock"],
    "Customers":  ["id", "name", "phone", "address"],
    "StockMoves": ["id", "ts", "kind", "product_id", "qty", "price_per_unit", "customer_id", "notes"],
    "Users":      ["id", "username", "password_hash", "salt"],
}

# Columns coerced to numeric on read
NUMERIC_COLUMNS = {
    "Products":   {"id", "opening_stock"},
    "Customers":  {"id"},
    "StockMoves": {"id", "product_id", "qty", "price_per_unit", "customer_id"},
    "Users":      {"id"},
}

# Map UI "tab" -> Supabase table name
def _table(tab_name: str) -> str:
    return {
        "Products":   "products",
        "Customers":  "customers",
        "StockMoves": "stock_moves",
        "Users":      "users",
    }[tab_name]

@st.cache_resource
def _client() -> Client:
    url = st.secrets.get("SUPABASE_URL")
    key = st.secrets.get("SUPABASE_SERVICE_ROLE_KEY") or st.secrets.get("SUPABASE_ANON_KEY")
    if not url or not key:
        st.error("❌ Supabase secrets missing. Check Streamlit Cloud > Settings > Secrets.")
        st.stop()
    return create_client(url, key)

def ensure_all_tabs():
    cli = _client()
    missing = []
    for tab in REQUIRED_TABS.keys():
        t = _table(tab)
        try:
            cli.table(t).select("id").limit(1).execute()
        except Exception:
            missing.append(t)
    if missing:
        st.error(f"❌ Supabase tables not accessible: {', '.join(missing)}. "
                 "Double-check schema + secrets.")
        st.stop()

# ---------- Reads ----------

@st.cache_data(ttl=15, show_spinner=False)
def fetch_df(tab_name: str) -> pd.DataFrame:
    """
    Read a table into a DataFrame with stable column order and numeric coercions.
    """
    cli = _client()
    t = _table(tab_name)

    # Default ordering per table
    order = {
        "Products":   ("name", True),
        "Customers":  ("name", True),
        "StockMoves": ("ts", True),  # ascending time
        "Users":      ("username", True),
    }[tab_name]

    try:
        q = cli.table(t).select("*")
        col, asc = order
        q = q.order(col, desc=not asc)
        data = q.execute().data or []
    except Exception:
        # Graceful fallback keeps UI functional
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

# ---------- Writes ----------

def append_row(tab_name: str, row: list):
    """
    Insert one row using the tab's column order.
    Example: append_row("Products", [1, "Renite", "Tiles", "600x600", "box", 0])
    """
    cli = _client()
    t = _table(tab_name)
    cols = REQUIRED_TABS[tab_name]

    rec = {}
    for i, col in enumerate(cols):
        rec[col] = row[i] if i < len(row) else None

    # Convert ISO strings for timestamps if needed
    if tab_name == "StockMoves" and isinstance(rec.get("ts"), str):
        try:
            # Ensure it's parseable; if it's already ISO it's fine
            datetime.fromisoformat(rec["ts"].replace("Z", "+00:00"))
        except Exception:
            # if not ISO, just set now
            rec["ts"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    cli.table(t).insert(rec).execute()
    st.cache_data.clear()  # invalidate cached reads after a write
