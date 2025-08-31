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

# === Canonical table schemas (match your Postgres table names exactly) ===
TABLE_COLUMNS: Dict[str, List[str]] = {
    "users":       ["id", "username", "password_hash", "salt"],
    "products":    ["id", "name", "material", "size", "unit", "opening_stock"],
    "customers":   ["id", "name", "phone", "address"],
    "suppliers":   ["id", "name", "phone", "address"],
    "payments":    ["id", "ts", "customer_id", "kind", "amount", "notes"],
    "stock_moves": ["id", "ts", "kind", "product_id", "qty", "price_per_unit", "customer_id", "supplier_id", "notes"],
}

# Allow legacy names / variants (TitleCase, no-underscore, etc.)
SYNONYMS: Dict[str, list[str]] = {
    "users":       ["Users"],
    "products":    ["Products"],
    "customers":   ["Customers"],
    "suppliers":   ["Suppliers"],
    "payments":    ["Payments"],
    "stock_moves": ["StockMoves", "stockmoves", "stock-moves"],
}

_client_cache = None

def _norm_name(name: str) -> str:
    return (name or "").strip().lower()

def _canon(name: str) -> str:
    n = _norm_name(name)
    if n in TABLE_COLUMNS:
        return n
    compact = n.replace("_", "").replace("-", "")
    for canon, alts in SYNONYMS.items():
        all_names = [canon] + alts
        for a in all_names:
            a_norm = _norm_name(a)
            if n == a_norm or compact == a_norm.replace("_", "").replace("-", ""):
                return canon
    raise ValueError(f"Unknown table: {name}")

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
    """Lazily create client using current env (so os.environ can be populated at runtime)."""
    global _client_cache
    if _client_cache is not None:
        return _client_cache

    url = os.getenv("SUPABASE_URL", "")
    key = (
        os.getenv("SUPABASE_KEY")
        or os.getenv("SUPABASE_ANON_KEY")
        or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or ""
    )
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
    for canon in TABLE_COLUMNS.keys():
        try:
            _ = sb.table(canon).select("*").limit(1).execute()
        except Exception as e:
            print(f"[ensure_all_tabs] Warning touching table {canon}: {e}")

def fetch_df(table_name: str) -> pd.DataFrame:
    t = _canon(table_name)
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

# ---- internal: compute next id for tables that aren't identity ----
def _next_id(table_name: str, sb: Client | None = None) -> int:
    t = _canon(table_name)
    sb = sb or _client()
    try:
        resp = sb.table(t).select("id").order("id", desc=True).limit(1).execute()
        if resp.data and resp.data[0].get("id") is not None:
            return int(resp.data[0]["id"]) + 1
    except Exception:
        pass
    return 1

def append_row(table_name: str, row_values: List[Any]) -> None:
    """
    Insert a single row. row_values must match the TABLE_COLUMNS order.
    Smart handling of 'id' across tables:
      * If 'id' is identity GENERATED ALWAYS → omit id.
      * If 'id' is NOT NULL and not identity → compute next id and send it.
    """
    t = _canon(table_name)
    sb = _client()

    cols = TABLE_COLUMNS[t]
    if len(row_values) != len(cols):
        raise ValueError(f"append_row: expected {len(cols)} values for table {t}, got {len(row_values)}")

    rec = {c: _noneify(v) for c, v in zip(cols, row_values)}

    # Normalize id field
    include_id = False
    if "id" in rec:
        if _is_blank(rec["id"]):
            include_id = False  # prefer to omit
        else:
            try:
                rec["id"] = int(rec["id"])
                include_id = True
            except Exception:
                include_id = False

    payload_no_id = {k: v for k, v in rec.items() if k != "id"}
    payload_with_id = rec

    # helper to perform insert
    def _insert(payload):
        return sb.table(t).insert(payload).execute()

    # 1) First attempt: if we *don't* trust id, try without id
    try:
        _insert(payload_with_id if include_id else payload_no_id)
        return
    except Exception as e1:
        msg = str(e1)

        # Identity ALWAYS complains when id is provided: 428C9 or "GENERATED ALWAYS"
        if ('428C9' in msg) or ('GENERATED ALWAYS' in msg) or ('non-DEFAULT value into column \"id\"' in msg):
            try:
                _insert(payload_no_id)
                return
            except Exception as e2:
                raise RuntimeError(f"append_row failed for {t}: {e2}") from e2

        # Not-null id complains when omitted: 23502 or message text
        if ('23502' in msg and 'column \"id\"' in msg) or ('null value in column \"id\"' in msg):
            try:
                nid = _next_id(t, sb)
                payload_with_id2 = dict(payload_with_id)
                payload_with_id2["id"] = nid
                _insert(payload_with_id2)
                return
            except Exception as e2:
                raise RuntimeError(f"append_row failed for {t}: {e2}") from e2

        # Unknown error → bubble up
        raise RuntimeError(f"append_row failed for {t}: {e1}") from e1

# ---------- helpers used by the login reset button ----------
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

    # Insert if missing — let DB decide id strategy via append_row
    append_row("users", [None, u, pwd_hash, salt])
