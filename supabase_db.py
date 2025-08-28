# supabase_db.py — thin Supabase wrapper used by app.py
import os
import pandas as pd
from typing import List, Dict, Any

try:
    from supabase import create_client, Client  # type: ignore
except Exception:
    create_client = None
    Client = None  # type: ignore

# ---- canonical, lowercase table names ----
TABLE_COLUMNS: Dict[str, List[str]] = {
    "users":       ["id", "username", "password_hash", "salt"],
    "products":    ["id", "name", "material", "size", "unit", "opening_stock"],
    "customers":   ["id", "name", "phone", "address"],
    "suppliers":   ["id", "name", "phone", "address"],
    "payments":    ["id", "ts", "customer_id", "kind", "amount", "notes"],
    "stock_moves": ["id", "ts", "kind", "product_id", "qty", "price_per_unit", "customer_id", "supplier_id", "notes"],
}

_client_cache = None

def _norm_name(name: str) -> str:
    return (name or "").strip().lower()

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

def _noneify(v: Any):
    """Convert pd.NA/NaN/'' to None to keep PostgREST happy."""
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    if isinstance(v, str) and v == "":
        return None
    return v

def _client():
    """
    Lazily create client using current env (so os.environ can be populated at runtime).
    """
    global _client_cache
    if _client_cache is not None:
        return _client_cache
    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_KEY", "")
    if not create_client or not url or not key:
        raise RuntimeError("Supabase credentials not configured (SUPABASE_URL / SUPABASE_KEY).")
    _client_cache = create_client(url, key)
    return _client_cache

def ensure_all_tabs():
    """Best-effort 'touch' of tables; no crash if creds missing."""
    try:
        sb = _client()
    except RuntimeError as e:
        # credentials missing; just log
        print(f"[ensure_all_tabs] Skipped: {e}")
        return
    for t in TABLE_COLUMNS:
        try:
            _ = sb.table(_norm_name(t)).select("*").limit(1).execute()
        except Exception as e:
            print(f"[ensure_all_tabs] Warning touching table {t}: {e}")

def fetch_df(table_name: str) -> pd.DataFrame:
    t = _norm_name(table_name)
    if t not in TABLE_COLUMNS:
        raise ValueError(f"Unknown table: {table_name}")
    try:
        sb = _client()
    except RuntimeError as e:
        print(f"[fetch_df] Skipped ({t}): {e}")
        return pd.DataFrame(columns=TABLE_COLUMNS[t])
    try:
        resp = sb.table(t).select("*").execute()
        data = resp.data or []
        df = pd.DataFrame(data)
        cols = TABLE_COLUMNS[t]
        for c in cols:
            if c not in df.columns:
                df[c] = pd.NA
        return df[cols]
    except Exception as e:
        print(f"[fetch_df] {t}: {e}")
        return pd.DataFrame(columns=TABLE_COLUMNS[t])

def append_row(table_name: str, row_values: List[Any]) -> None:
    """
    Insert a single row. row_values must match the TABLE_COLUMNS order.
    NA-safe handling for ids and other nullable fields.
    """
    t = _norm_name(table_name)
    if t not in TABLE_COLUMNS:
        raise ValueError(f"Unknown table: {table_name}")
    sb = _client()  # let this raise if creds missing

    cols = TABLE_COLUMNS[t]
    if len(row_values) != len(cols):
        raise ValueError(f"append_row: expected {len(cols)} values for table {t}, got {len(row_values)}")

    rec = {c: _noneify(v) for c, v in zip(cols, row_values)}

    # keep id clean (int or None)
    if "id" in rec and _is_blank(rec["id"]):
        rec["id"] = None
    elif "id" in rec and rec["id"] is not None:
        try:
            rec["id"] = int(rec["id"])
        except Exception:
            rec["id"] = None

    try:
        _ = sb.table(t).insert(rec).execute()
    except Exception as e:
        raise RuntimeError(f"append_row failed for {t}: {e}") from e
