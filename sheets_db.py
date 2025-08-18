import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound

SHEET_TITLE = "Inventory"  # your Google Sheet name

REQUIRED_TABS = {
    # tab_name: header row
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
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)

def _open_sheet():
    client = _client()
    return client.open(SHEET_TITLE)

def _ensure_tab(sh, tab_name: str, headers: list[str]):
    try:
        ws = sh.worksheet(tab_name)
    except WorksheetNotFound:
        ws = sh.add_worksheet(title=tab_name, rows=100, cols=max(8, len(headers)))
        ws.update("A1", [headers])
        return ws

    # ensure header row matches (if empty or different)
    current = ws.row_values(1)
    if current != headers:
        ws.clear()
        ws.update("A1", [headers])
    return ws

def ensure_all_tabs():
    sh = _open_sheet()
    for tab, headers in REQUIRED_TABS.items():
        _ensure_tab(sh, tab, headers)

def _ws(tab_name: str):
    sh = _open_sheet()
    headers = REQUIRED_TABS[tab_name]
    return _ensure_tab(sh, tab_name, headers)

def fetch_df(tab_name: str) -> pd.DataFrame:
    ws = _ws(tab_name)
    rows = ws.get_all_records()  # list[dict], uses header row
    df = pd.DataFrame(rows)
    if df.empty:
        df = pd.DataFrame(columns=REQUIRED_TABS[tab_name])

    # coerce numeric cols
    for col in NUMERIC_COLUMNS.get(tab_name, set()):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # strip strings
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str).str.strip()
    return df

def append_row(tab_name: str, row: list):
    ws = _ws(tab_name)
    ws.append_row(row, value_input_option="USER_ENTERED")
