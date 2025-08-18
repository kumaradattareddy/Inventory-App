# sheets_db.py — streamlined + resilient

import time
import string
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound, SpreadsheetNotFound, APIError

# ---- Expected headers (row 1) ----
REQUIRED_TABS = {
    "Products":   ["id", "name", "material", "size", "unit", "opening_stock"],
    "Customers":  ["id", "name", "phone", "address"],
    "StockMoves": ["id", "ts", "kind", "product_id", "qty", "price_per_unit", "customer_id", "notes"],
    "Users":      ["id", "username", "password_hash", "salt"],
}
NUMERIC_COLUMNS = {
    "Products":   {"id", "opening_stock"},
    "Customers":  {"id"},
    "StockMoves": {"id", "product_id", "qty", "price_per_unit", "customer_id"},
    "Users":      {"id"},
}

# ---------- Auth / client ----------

@st.cache_resource
def _client():
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)

def _retry(fn, attempts: int = 4, delay: float = 0.8):
    last_err = None
    for i in range(attempts):
        try:
            return fn()
        except APIError as e:
            last_err = e
            time.sleep(delay * (1.6 ** i))
    email = st.secrets["gcp_service_account"].get("client_email", "service-account")
    raise RuntimeError(
        "Google Sheets API error.\n\n"
        "- If your Google storage is full, Google may reject reads/writes.\n"
        f"- Confirm the sheet is shared with: {email} (Editor)\n"
        "- Ensure **Google Drive API** and **Google Sheets API** are enabled\n"
        "- Try again shortly (temporary API hiccup)\n"
    ) from last_err

# ---------- Open spreadsheet (cached) ----------

@st.cache_resource
def _sheet():
    client = _client()
    sheet_id  = st.secrets.get("SHEET_ID") or st.secrets.get("GOOGLE_SHEET_ID")
    sheet_url = st.secrets.get("SHEET_URL")

    last_err = None
    if sheet_id:
        try:
            return _retry(lambda: client.open_by_key(sheet_id))
        except (SpreadsheetNotFound, APIError) as e:
            last_err = e
    if sheet_url:
        try:
            return _retry(lambda: client.open_by_url(sheet_url))
        except (SpreadsheetNotFound, APIError) as e:
            last_err = e

    email = st.secrets["gcp_service_account"].get("client_email", "service-account")
    raise RuntimeError(
        "Could not open Google Sheet.\n\n"
        f"- Shared with: {email} (Editor)\n"
        "- Drive API + Sheets API enabled\n"
        "- Verify SHEET_ID and/or SHEET_URL in secrets"
    ) from last_err

def _ws(tab_name: str):
    """Get/create worksheet. We set headers only when creating; no per-read header checks."""
    sh = _sheet()
    headers = REQUIRED_TABS[tab_name]
    try:
        return _retry(lambda: sh.worksheet(tab_name))
    except WorksheetNotFound:
        ws = _retry(lambda: sh.add_worksheet(title=tab_name, rows=200, cols=max(10, len(headers))))
        _retry(lambda: ws.update("A1", [headers]))
        return ws

def ensure_all_tabs():
    """Call once from app.py on start; creates tabs if missing and seeds headers when empty."""
    try:
        sh = _sheet()
    except Exception as e:
        st.error(str(e))
        st.stop()
    for tab, headers in REQUIRED_TABS.items():
        try:
            ws = _retry(lambda: sh.worksheet(tab))
        except WorksheetNotFound:
            ws = _retry(lambda: sh.add_worksheet(title=tab, rows=200, cols=max(10, len(headers))))
            _retry(lambda: ws.update("A1", [headers]))
            continue
        # If first row is empty, seed headers; otherwise leave user data intact
        try:
            first_row = _retry(lambda: ws.get_values("A1:A1"))
            if not first_row:
                _retry(lambda: ws.update("A1", [headers]))
        except Exception:
            pass  # don't fail startup on a read glitch

# ---------- Reads (fast + cached) ----------

def _col_letter(n: int) -> str:
    # 1 -> A, 2 -> B, ... supports up to ZZ
    s, n = "", int(n)
    while n:
        n, r = divmod(n - 1, 26)
        s = string.ascii_uppercase[r] + s
    return s

@st.cache_data(ttl=30, show_spinner=False)
def _fetch_values(tab_name: str, a1_range: str):
    ws = _ws(tab_name)
    return _retry(lambda: ws.get_values(a1_range))

def fetch_df(tab_name: str) -> pd.DataFrame:
    headers = REQUIRED_TABS[tab_name]
    last_col = _col_letter(len(headers))
    a1 = f"A1:{last_col}10000"  # bounded range for speed

    # Safe fetch — if it fails, return empty frame so UI stays up
    try:
        values = _fetch_values(tab_name, a1)
    except Exception as e:
        st.warning(f"Sheets temporarily unavailable for '{tab_name}'. Showing empty data.")
        return pd.DataFrame(columns=headers)

    if not values:
        return pd.DataFrame(columns=headers)

    header = values[0]
    rows = values[1:] if len(values) > 1 else []

    if header != headers:
        # Non-fatal: still build a DataFrame so screens render; show a hint
        st.warning(f"'{tab_name}' headers differ.\nFound: {header}\nExpected: {headers}")

    df = pd.DataFrame(rows, columns=header)
    # Normalize to expected columns if header differs
    for col in headers:
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

    return df[headers]  # keep column order stable

# ---------- Writes ----------

def append_row(tab_name: str, row: list):
    ws = _ws(tab_name)
    _retry(lambda: ws.append_row(row, value_input_option="USER_ENTERED"))
    st.cache_data.clear()  # invalidate cached reads after a write
