import time
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound, SpreadsheetNotFound, APIError

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
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)

def _retry(fn, attempts=4, delay=0.8):
    last_err = None
    for i in range(attempts):
        try:
            return fn()
        except APIError as e:
            last_err = e
            time.sleep(delay * (1.6 ** i))
    if last_err:
        # Helpful final message
        email = st.secrets["gcp_service_account"].get("client_email", "service-account")
        raise RuntimeError(
            "Google Sheets API error.\n\n"
            "- If your Google account storage is full, Sheets rejects edits/reads.\n"
            "- Confirm the sheet is shared with: " + email + " (Editor)\n"
            "- Ensure **Google Drive API** and **Google Sheets API** are enabled\n"
            "- Try again in a moment (temporary API hiccup)\n"
        ) from last_err
    raise RuntimeError("Unknown error when calling Google Sheets API.")

def _open_sheet():
    """
    Open using SHEET_ID (preferred). If that fails, try SHEET_URL.
    We never fall back to a bare title.
    """
    client = _client()
    sheet_id  = st.secrets.get("SHEET_ID") or st.secrets.get("GOOGLE_SHEET_ID")
    sheet_url = st.secrets.get("SHEET_URL")

    if sheet_id:
        try:
            return _retry(lambda: client.open_by_key(sheet_id))
        except SpreadsheetNotFound as e:
            email = st.secrets["gcp_service_account"].get("client_email", "service-account")
            raise RuntimeError(
                f"SHEET_ID found but not accessible. Share the sheet with {email} (Editor)."
            ) from e

    if sheet_url:
        return _retry(lambda: client.open_by_url(sheet_url))

    raise RuntimeError(
        'Missing SHEET_ID (or SHEET_URL) in Streamlit secrets. '
        'Add: SHEET_ID = "your-google-sheet-id" at the top level.'
    )

def _ensure_tab(sh, tab_name: str, headers: list[str]):
    """
    Ensure the tab exists and headers are correct.
    We DO NOT clear non-matching headers automatically to avoid data loss.
    """
    try:
        ws = _retry(lambda: sh.worksheet(tab_name))
    except WorksheetNotFound:
        ws = _retry(lambda: sh.add_worksheet(title=tab_name, rows=200, cols=max(10, len(headers))))
        _retry(lambda: ws.update("A1", [headers]))
        return ws

    current = _retry(lambda: ws.row_values(1))
    if not current:
        _retry(lambda: ws.update("A1", [headers]))
        return ws

    if current != headers:
        raise RuntimeError(
            f"Tab '{tab_name}' header mismatch.\n"
            f"Found: {current}\nExpected: {headers}\n"
            "Fix row-1 headers in the sheet (exact text/order) or start with a blank sheet."
        )
    return ws

def ensure_all_tabs():
    try:
        sh = _open_sheet()
    except Exception as e:
        st.error(str(e))
        st.stop()
    for tab, headers in REQUIRED_TABS.items():
        _ensure_tab(sh, tab, headers)

def _ws(tab_name: str):
    sh = _open_sheet()
    headers = REQUIRED_TABS[tab_name]
    return _ensure_tab(sh, tab_name, headers)

def fetch_df(tab_name: str) -> pd.DataFrame:
    ws = _ws(tab_name)
    rows = _retry(lambda: ws.get_all_records())
    df = pd.DataFrame(rows)
    if df.empty:
        df = pd.DataFrame(columns=REQUIRED_TABS[tab_name])

    # Coerce numerics
    for col in NUMERIC_COLUMNS.get(tab_name, set()):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Trim strings
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str).str.strip()
    return df

def append_row(tab_name: str, row: list):
    ws = _ws(tab_name)
    _retry(lambda: ws.append_row(row, value_input_option="USER_ENTERED"))
