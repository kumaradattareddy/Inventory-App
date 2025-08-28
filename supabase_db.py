# supabase_db.py — thin Supabase wrapper used by app.py
import os
import pandas as pd
import hashlib, secrets
from typing import List, Dict, Any

try:
    from supabase import create_client, Client  # type: ignore
except Exception:
    create_client = None
    Client = None  # type: ignore

# --- Accept old TitleCase names but canonicalize to lowercase internally ---
_TABLES_RAW: Dict[str, List[str]] = {
    "Users":       ["id", "username", "password_hash", "salt"],
    "Products":    ["id", "name", "material", "size", "unit", "opening_stock"],
    "Customers":   ["id", "name", "phone", "address"],
    "Suppliers":   ["id", "name", "phone", "address"],
    "Payments":    ["id", "ts", "customer_id", "kind", "amount", "notes"],
    "StockMoves":  ["id", "ts", "kind", "product_id", "qty", "price_per_unit", "customer_id", "supplier_id", "notes"],
}
TABLE_COLUMNS: Dict[str, List[str]] = {k.lower(): v for k, v in _TABLES_RAW.items()}

_client_cache = None

def _norm_name(name: str) -> str:
    return (name or "").strip().lower()

def _is_blank(v: Any) -> bool:
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

# ---------- helpers used by the login reset button ----------
def _next_id(table_name: str) -> int:
    """Find next id for a table (works even if id isn't SERIAL)."""
    t = _norm_name(table_name)
    sb = _client()
    try:
        resp = sb.table(t).select("id").order("id", desc=True).limit(1).execute()
        if resp.data and resp.data[0].get("id") is not None:
            return int(resp.data[0]["id"]) + 1
    except Exception:
        pass
    return 1

def reset_or_create_user(username: str, password: str) -> None:
    """
    Ensure a user row exists for `username` with PBKDF2(password, salt, 100k).
    If the user exists → UPDATE salt/hash. Otherwise → INSERT a new row.
    """
    t = "users"
    sb = _client()
    u = (username or "").strip().lower()

    salt = secrets.token_hex(16)
    pwd_hash = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), bytes.fromhex(salt), 100_000).hex()

    # Try update first
    try:
        resp = sb.table(t).update({"salt": salt, "password_hash": pwd_hash}).eq("username", u).execute()
        if getattr(resp, "data", None):  # updated existing row
            return
    except Exception:
        pass

    # Insert if missing
    new_id = _next_id(t)
    sb.table(t).insert({
        "id": new_id,
        "username": u,
        "salt": salt,
        "password_hash": pwd_hash
    }).execute()
