# supabase_db.py — thin Supabase wrapper used by app.py
import os
import pandas as pd
from typing import List, Dict, Any

try:
    from supabase import create_client, Client  # type: ignore
except Exception:
    create_client = None
    Client = None  # type: ignore

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

TABLE_COLUMNS: Dict[str, List[str]] = {
    "Users":       ["id", "username", "password_hash", "salt"],
    "Products":    ["id", "name", "material", "size", "unit", "opening_stock"],
    "Customers":   ["id", "name", "phone", "address"],
    "Suppliers":   ["id", "name", "phone", "address"],
    "Payments":    ["id", "ts", "customer_id", "kind", "amount", "notes"],
    "StockMoves":  ["id", "ts", "kind", "product_id", "qty", "price_per_unit", "customer_id", "supplier_id", "notes"],
}

_client_cache = None

def _client():
    global _client_cache
    if _client_cache is not None:
        return _client_cache
    if not create_client or not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Supabase credentials not configured (SUPABASE_URL / SUPABASE_KEY).")
    _client_cache = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _client_cache

def _is_blank(v: Any) -> bool:
    """NA-safe 'is empty' check."""
    if v is None:
        return True
    if isinstance(v, str) and v == "":
        return True
    try:
        return pd.isna(v)
    except Exception:
        return False

def ensure_all_tabs():
    """Best-effort 'touch' of tables so startup doesn't crash if any is missing."""
    sb = _client()
    for t in TABLE_COLUMNS:
        try:
            # small, cheap read
            _ = sb.table(t).select("*").limit(1).execute()
        except Exception as e:
            # Don't crash the app here; schema should be created out-of-band.
            print(f"[ensure_all_tabs] Warning touching table {t}: {e}")

def fetch_df(table_name: str) -> pd.DataFrame:
    sb = _client()
    if table_name not in TABLE_COLUMNS:
        raise ValueError(f"Unknown table: {table_name}")
    try:
        resp = sb.table(table_name).select("*").execute()
        data = resp.data or []
        df = pd.DataFrame(data)
        # normalize column order
        cols = TABLE_COLUMNS[table_name]
        for c in cols:
            if c not in df.columns:
                df[c] = pd.NA
        return df[cols]
    except Exception as e:
        print(f"[fetch_df] {table_name}: {e}")
        return pd.DataFrame(columns=TABLE_COLUMNS[table_name])

def append_row(table_name: str, row_values: List[Any]) -> None:
    """
    Insert a single row. row_values must match the TABLE_COLUMNS order.
    NA-safe handling for 'id' to avoid 'boolean value of NA is ambiguous'.
    """
    sb = _client()
    if table_name not in TABLE_COLUMNS:
        raise ValueError(f"Unknown table: {table_name}")

    cols = TABLE_COLUMNS[table_name]
    if len(row_values) != len(cols):
        raise ValueError(f"append_row: expected {len(cols)} values for table {table_name}, got {len(row_values)}")

    rec = {c: v for c, v in zip(cols, row_values)}

    # === FIX: NA-safe id normalization ===
    if "id" in rec and _is_blank(rec["id"]):
        rec["id"] = None

    # Normalize empty strings to None for nullable fields for cleaner storage
    for k, v in list(rec.items()):
        if v == "":
            rec[k] = None

    try:
        _ = sb.table(table_name).insert(rec).execute()
    except Exception as e:
        # Helpful debug for duplicate key etc.
        raise RuntimeError(f"append_row failed for {table_name}: {e}") from e
