import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound, SpreadsheetNotFound

# You can override these via secrets
DEFAULT_SHEET_TITLE = "Inventory"

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

@st.cache_resource
def _client():
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=["https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"],
    )
    return gspread.authorize(creds)

def _open_sheet():
    """Open the spreadsheet using SHEET_ID (preferred), SHEET_URL, or fallback to title."""
    client = _client()

    sheet_id = st.secrets.get("SHEET_ID") or st.secrets.get("GOOGLE_SHEET_ID")
    sheet_url = st.secrets.get("SHEET_URL")
    sheet_title = st.secrets.get("SHEET_TITLE", DEFAULT_SHEET_TITLE)

    if sheet_id:
        return client.open_by_key(sheet_id)
    if sheet_url:
        # allow full URL in secrets too
        return client.open_by_url(sheet_url)
    return client.open(sheet_title)  # fallback to title

def _ensure_tab(sh, tab_name: str, headers: list[str]):
    try:
        ws = sh.worksheet(tab_name)
    except WorksheetNotFound:
        ws = sh.add_worksheet(title=tab_name, rows=100, cols=max(8, len(headers)))
        ws.update("A1", [headers])
        return ws

    # make sure header row matches
    current = ws.row_values(1)
    if current != headers:
        ws.clear()
        ws.update("A1", [headers])
    return ws

def ensure_all_tabs():
    try:
        sh = _open_sheet()
    except SpreadsheetNotFound:
        # Give a clear message in the app UI
        raise RuntimeError(
            "Spreadsheet not found. Share your Google Sheet with the service account "
            "and/or set SHEET_ID in your Streamlit secrets."
        )
    for tab, headers in REQUIRED_TABS.items():
        _ensure_tab(sh, tab, headers)

def _ws(tab_name: str):
    sh = _open_sheet()
    headers = REQUIRED_TABS[tab_name]
    return _ensure_tab(sh, tab_name, headers)

def fetch_df(tab_name: str) -> pd.DataFrame:
    ws = _ws(tab_name)
    rows = ws.get_all_records()
    df = pd.DataFrame(rows)
    if df.empty:
        df = pd.DataFrame(columns=REQUIRED_TABS[tab_name])

    for col in NUMERIC_COLUMNS.get(tab_name, set()):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str).str.strip()
    return df

def append_row(tab_name: str, row: list):
    ws = _ws(tab_name)
    ws.append_row(row, value_input_option="USER_ENTERED")
