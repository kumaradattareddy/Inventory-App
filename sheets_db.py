import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound, SpreadsheetNotFound, APIError

# Tabs/columns the app expects
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

def _open_sheet():
    """
    Open using SHEET_ID (preferred) or SHEET_URL from secrets.
    We do NOT fall back to title to avoid silent mismatches.
    """
    client = _client()
    sheet_id  = st.secrets.get("SHEET_ID") or st.secrets.get("GOOGLE_SHEET_ID")
    sheet_url = st.secrets.get("SHEET_URL")

    if sheet_id:
        try:
            return client.open_by_key(sheet_id)
        except SpreadsheetNotFound as e:
            email = st.secrets["gcp_service_account"].get("client_email", "service-account")
            raise RuntimeError(
                f"SHEET_ID found but not accessible. Share the sheet with {email} (Editor)."
            ) from e
        except APIError as e:
            raise RuntimeError("Google API error opening by SHEET_ID. Check Drive API + sharing.") from e

    if sheet_url:
        try:
            return client.open_by_url(sheet_url)
        except Exception as e:
            raise RuntimeError("Invalid SHEET_URL or no access. Check sharing & URL.") from e

    raise RuntimeError(
        'Missing SHEET_ID (or SHEET_URL) in Streamlit secrets. '
        'Add: SHEET_ID = "your-google-sheet-id" at top level.'
    )

def _ensure_tab(sh, tab_name: str, headers: list[str]):
    """
    Ensure the worksheet exists and has the expected header row.
    If headers differ AND the sheet already has a header, we do NOT clear it
    (to avoid data loss); we raise a helpful error instead.
    """
    try:
        ws = sh.worksheet(tab_name)
    except WorksheetNotFound:
        ws = sh.add_worksheet(title=tab_name, rows=200, cols=max(10, len(headers)))
        ws.update("A1", [headers])
        return ws

    current = ws.row_values(1)
    if not current:
        ws.update("A1", [headers])
        return ws

    if current != headers:
        raise RuntimeError(
            f"Tab '{tab_name}' exists but header mismatch.\n"
            f"Found: {current}\nExpected: {headers}\n\n"
            f"Fix headers in the sheet (row 1) OR create a new blank sheet."
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
    rows = ws.get_all_records()
    df = pd.DataFrame(rows)
    if df.empty:
        df = pd.DataFrame(columns=REQUIRED_TABS[tab_name])

    # Coerce numeric columns
    for col in NUMERIC_COLUMNS.get(tab_name, set()):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Strip whitespace in strings
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str).str.strip()
    return df

def append_row(tab_name: str, row: list):
    ws = _ws(tab_name)
    ws.append_row(row, value_input_option="USER_ENTERED")
