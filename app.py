# app.py — Streamlit UI (Supabase backend, with Customers, Suppliers, Payments, Products, Purchases/Sales)
import pandas as pd
import streamlit as st
import hashlib, secrets
from datetime import date, datetime, timedelta

from supabase_db import ensure_all_tabs, fetch_df, append_row

# ===================== App Config / Auth =====================
st.set_page_config(page_title="Tiles & Granite Inventory", layout="wide")

# ======= SINGLE-USER CONFIG (must be lowercase) =======
ALLOWED_USERS = {"venkat reddy"}
DEFAULT_USERNAME = "venkat reddy"
DEFAULT_PASSWORD = "1234"
# ======================================================

# ---------- Styling ----------
st.markdown("""
<style>
html, body, [class*="css"]  { font-size: 18px !important; }
button, .stButton button { padding: 0.6rem 1rem !important; font-size: 18px !important; }
label { font-size: 18px !important; }
.negative { color: #b00020; font-weight: 700; }
.amount { font-weight: 700; }
[data-testid="stForm"] { padding: 0.75rem 1rem; border-radius: 12px; border: 1px solid rgba(255,255,255,0.08); }
</style>
""", unsafe_allow_html=True)

# ---- scheduled widget resets (prevents "cannot be modified after widget..." error) ----
def _apply_scheduled_resets():
    keys = st.session_state.pop("_reset_keys", None)
    if keys:
        for k in set(keys):
            st.session_state.pop(k, None)
_apply_scheduled_resets()

def _schedule_reset(*keys):
    pending = set(st.session_state.get("_reset_keys", []))
    pending.update(keys)
    st.session_state["_reset_keys"] = list(pending)

# ===================== Ensure tables exist =====================
ensure_all_tabs()

# ===================== Cached reads =====================
@st.cache_data(ttl=12, show_spinner=False)
def users_df():
    df = fetch_df("Users")
    if not df.empty:
        df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
    return df

@st.cache_data(ttl=12, show_spinner=False)
def products_df():
    df = fetch_df("Products")
    if not df.empty:
        df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
        df["opening_stock"] = pd.to_numeric(df["opening_stock"], errors="coerce").fillna(0.0)
    return df

@st.cache_data(ttl=12, show_spinner=False)
def customers_df():
    df = fetch_df("Customers")
    if not df.empty:
        df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
    return df

@st.cache_data(ttl=12, show_spinner=False)
def suppliers_df():
    df = fetch_df("Suppliers")
    if not df.empty:
        df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
    return df

@st.cache_data(ttl=12, show_spinner=False)
def payments_df():
    df = fetch_df("Payments")
    if not df.empty:
        df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
        df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").astype("Int64")
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    return df

@st.cache_data(ttl=12, show_spinner=False)
def stock_moves_df():
    df = fetch_df("StockMoves")
    if not df.empty:
        for col in ["id", "product_id", "customer_id", "supplier_id"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0.0)
        df["price_per_unit"] = pd.to_numeric(df["price_per_unit"], errors="coerce").fillna(0.0)
    return df

def _clear_caches():
    st.cache_data.clear()

# ===================== AUTH helpers =====================
def _hash_password(password: str, salt: str) -> str:
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), bytes.fromhex(salt), 100_000)
    return dk.hex()

def _next_id(tab: str) -> int:
    df = fetch_df(tab)
    if df.empty or "id" not in df.columns:
        return 1
    return int(pd.to_numeric(df["id"], errors="coerce").fillna(0).max()) + 1

def user_exists(username: str) -> bool:
    df = users_df()
    if df.empty:
        return False
    u = username.strip().lower()
    return any(df["username"].astype(str).str.lower() == u)

def create_user(username: str, password: str):
    salt = secrets.token_hex(16)
    pwd_hash = _hash_password(password, salt)
    new_id = _next_id("Users")
    append_row("Users", [new_id, username.strip().lower(), pwd_hash, salt])
    _clear_caches()

def verify_login(username: str, password: str):
    username = username.strip().lower()
    if username not in ALLOWED_USERS:
        return None
    df = users_df()
    if df.empty:
        return None
    row = df[df["username"].astype(str).str.lower() == username]
    if row.empty:
        return None
    row = row.iloc[0]
    if _hash_password(password, row["salt"]) == row["password_hash"]:
        return {"username": row["username"]}
    return None

# Ensure default user exists
if DEFAULT_USERNAME in ALLOWED_USERS and not user_exists(DEFAULT_USERNAME):
    try:
        create_user(DEFAULT_USERNAME, DEFAULT_PASSWORD)
    except Exception:
        pass

# ===================== Data helpers =====================
def list_products():
    df = products_df()
    return [] if df.empty else df.to_dict(orient="records")

def list_customers():
    df = customers_df()
    return [] if df.empty else df.to_dict(orient="records")

def list_suppliers():
    df = suppliers_df()
    return [] if df.empty else df.to_dict(orient="records")

def _get_opening_stock(product_id: int) -> float:
    df = products_df()
    if df.empty:
        return 0.0
    row = df[df["id"] == product_id]
    if row.empty:
        return 0.0
    return float(row.iloc[0]["opening_stock"] or 0.0)

def product_stock(product_id: int) -> float:
    opening = _get_opening_stock(product_id)
    moves = stock_moves_df()
    if moves.empty:
        return float(opening)
    s = float(moves[moves["product_id"] == product_id]["qty"].sum() or 0.0)
    return float(opening) + s

def add_product(name, material, size, unit, opening_stock):
    new_id = _next_id("Products")
    append_row("Products", [
        new_id,
        (name or "").strip(),
        (material or "").strip() or None,
        (size or "").strip() or None,
        (unit or "").strip(),
        float(opening_stock or 0.0)
    ])
    _clear_caches()
    return new_id

def add_customer(name, phone, address):
    new_id = _next_id("Customers")
    append_row("Customers", [
        new_id,
        (name or "").strip(),
        (phone or "").strip() or None,
        (address or "").strip() or None
    ])
    _clear_caches()
    return new_id

def add_supplier(name, phone, address):
    new_id = _next_id("Suppliers")
    append_row("Suppliers", [
        new_id,
        (name or "").strip(),
        (phone or "").strip() or None,
        (address or "").strip() or None
    ])
    _clear_caches()
    return new_id

def add_payment(customer_id: int, kind: str, amount: float, notes: str = None,
                when: datetime | None = None, dedupe_window_seconds: int = 120) -> bool:
    """Record payment/advance/opening_due. Positive amount for payment/advance; opening_due is also positive here."""
    if not customer_id or amount == 0:
        return False
    ts_dt = (when or datetime.now())
    # dedupe
    if dedupe_window_seconds and dedupe_window_seconds > 0:
        since = ts_dt - timedelta(seconds=dedupe_window_seconds)
        pay = payments_df()
        if not pay.empty:
            pay = pay.copy()
            pay["ts_dt"] = pd.to_datetime(pay["ts"], errors="coerce")
            dup = pay[
                (pay["ts_dt"] >= since) &
                (pay["customer_id"] == int(customer_id)) &
                (pay["amount"] == float(amount)) &
                (pay["kind"].fillna("") == (kind or "")) &
                (pay["notes"].fillna("") == (notes or ""))
            ]
            if not dup.empty:
                return False
    new_id = _next_id("Payments")
    ts = ts_dt.isoformat(timespec="seconds")
    append_row("Payments", [new_id, ts, int(customer_id), kind, float(amount), (notes or None)])
    _clear_caches()
    return True

def add_move(kind, product_id, qty, price_per_unit=None, customer_id=None, supplier_id=None, notes=None,
             when: datetime | None = None, dedupe_window_seconds: int = 120) -> bool:
    """
    Insert a stock move. Returns True if inserted, False if skipped as duplicate.
    For sales → pass customer_id; for purchases → pass supplier_id.
    Dedupes identical moves within the last dedupe_window_seconds.
    """
    ts_dt = (when or datetime.now())
    ins_qty = -qty if (kind == "sale" and qty > 0) else qty

    if dedupe_window_seconds and dedupe_window_seconds > 0:
        since = ts_dt - timedelta(seconds=dedupe_window_seconds)
        df = stock_moves_df()
        if not df.empty:
            df = df.copy()
            df["ts_dt"] = pd.to_datetime(df["ts"], errors="coerce")
            dup = df[
                (df["ts_dt"] >= since) &
                (df["kind"] == kind) &
                (df["product_id"] == int(product_id)) &
                (df["qty"] == float(ins_qty)) &
                (df["price_per_unit"].fillna(0.0) == float(price_per_unit or 0.0)) &
                ((df["customer_id"].fillna(-1)) == (int(customer_id) if customer_id is not None else -1)) &
                ((df["supplier_id"].fillna(-1)) == (int(supplier_id) if supplier_id is not None else -1)) &
                (df["notes"].fillna("") == (notes or ""))
            ]
            if not dup.empty:
                return False

    new_id = _next_id("StockMoves")
    ts = ts_dt.isoformat(timespec="seconds")
    append_row("StockMoves", [
        new_id, ts, kind, int(product_id), float(ins_qty),
        (float(price_per_unit) if price_per_unit not in (None, "") else None),
        (int(customer_id) if customer_id not in (None, "") else None),
        (int(supplier_id) if supplier_id not in (None, "") else None),
        (notes or None)
    ])
    _clear_caches()
    return True

def products_lookup_key(name: str, size: str, unit: str):
    return (name or "").strip().lower(), (size or "").strip().lower(), (unit or "").strip()

def get_product_by_name_size_unit(name: str, size: str, unit: str):
    df = products_df()
    if df.empty:
        return None
    n, s, u = products_lookup_key(name, size, unit)
    mask = (
        (df["name"].astype(str).str.lower() == n) &
        (df["unit"].astype(str) == u) &
        (df["size"].fillna("").astype(str).str.lower() == s)
    )
    row = df[mask]
    return None if row.empty else row.iloc[0].to_dict()

def ensure_product(name: str, size: str, unit: str, material: str = None, opening_stock: float = 0.0):
    p = get_product_by_name_size_unit(name, size, unit)
    if p:
        return int(p["id"])
    return add_product(name=name, material=material, size=size, unit=unit, opening_stock=opening_stock)

def ensure_customer_by_name(name: str, phone: str = None, address: str = None):
    nm = (name or "").strip()
    if not nm:
        return None
    df = customers_df()
    if not df.empty:
        row = df[df["name"].astype(str).str.lower() == nm.lower()]
        if not row.empty:
            return int(row.iloc[0]["id"])
    return add_customer(nm, phone, address)

def ensure_supplier_by_name(name: str, phone: str = None, address: str = None):
    nm = (name or "").strip()
    if not nm:
        return None
    df = suppliers_df()
    if not df.empty:
        row = df[df["name"].astype(str).str.lower() == nm.lower()]
        if not row.empty:
            return int(row.iloc[0]["id"])
    return add_supplier(nm, phone, address)

def customer_balance(customer_id: int) -> float:
    """
    Outstanding = Σ(sales amount) + Σ(opening_due) − Σ(payments) − Σ(advances).
    Negative result means advance/credit available.
    """
    if not customer_id:
        return 0.0

    mv = stock_moves_df()
    sales_total = 0.0
    if not mv.empty:
        s = mv[(mv["kind"] == "sale") & (mv["customer_id"] == int(customer_id))].copy()
        if not s.empty:
            s["price_per_unit"] = pd.to_numeric(s["price_per_unit"], errors="coerce").fillna(0.0)
            s["qty"] = pd.to_numeric(s["qty"], errors="coerce").fillna(0.0).abs()
            sales_total = float((s["qty"] * s["price_per_unit"]).sum())

    pay = payments_df()
    opening_due = payments = advances = 0.0
    if not pay.empty:
        p = pay[pay["customer_id"] == int(customer_id)]
        opening_due = float(p[p["kind"] == "opening_due"]["amount"].sum() or 0.0)
        payments    = float(p[p["kind"] == "payment"]["amount"].sum() or 0.0)
        advances    = float(p[p["kind"] == "advance"]["amount"].sum() or 0.0)

    return round(sales_total + opening_due - payments - advances, 2)

def _to_float(txt: str) -> float:
    s = (txt or "").strip()
    if not s:
        return 0.0
    try:
        return float(s)
    except:
        return 0.0

# ===================== UI =====================
st.title("Tiles & Granite Inventory")

# ---- LOGIN WALL ----
if "user" not in st.session_state:
    with st.expander("🔐 Login", expanded=True):
        u = st.text_input("Username")
        p = st.text_input("Password", type="password")
        if st.button("Login"):
            user = verify_login(u, p)
            if user:
                st.session_state.user = user
                st.rerun()
            else:
                st.error("Invalid credentials.")
    st.stop()

# Logged in – top-right logout
top_left, top_right = st.columns([6, 1])
with top_left:
    st.success(f"Logged in as **{st.session_state.user['username']}**")
with top_right:
    if st.button("Logout"):
        st.session_state.pop("user", None)
        st.rerun()

# ---------- ROW FORM ----------
DEFAULT_UNIT_OPTIONS = ["box", "pcs", "sq_ft", "bag", "kg"]
DEFAULT_MATERIAL_OPTIONS = ["Tiles", "Granite", "Marble", "Other"]

def ensure_rows(session_key: str, start_rows: int = 6):
    if session_key not in st.session_state:
        st.session_state[session_key] = [
            {"material":"","product_name":"","size":"","unit":"","qty":"","rate":""} for _ in range(start_rows)
        ]

def _row_amount(qty_txt: str, rate_txt: str) -> float:
    try:
        return float(qty_txt) * float(rate_txt)
    except:
        return 0.0

def row_form(session_key: str, title: str):
    ensure_rows(session_key)
    rows = st.session_state[session_key]

    st.markdown(f"#### {title}")

    c1, c2, _ = st.columns([1,1,6])
    with c1:
        if st.button("➕ Add row", key=f"add_{session_key}"):
            rows.append({"material":"","size":"","product_name":"","unit":"","qty":"","rate":""})
            st.rerun()
    with c2:
        if st.button("🧹 Clear", key=f"clear_{session_key}"):
            st.session_state[session_key] = [
                {"material":"","size":"","product_name":"","unit":"","qty":"","rate":""} for _ in range(6)
            ]
            st.rerun()
    st.caption("Tip: type freely; the table won’t refresh until you click **Update Items**.")

    subtotal_key = f"{session_key}_subtotal"
    if subtotal_key not in st.session_state:
        st.session_state[subtotal_key] = 0.0

    with st.form(f"form_{session_key}", clear_on_submit=False):
        labs = st.columns([1.1, 1.1, 2, 0.9, 0.8, 0.9, 1.1])
        labs[0].markdown("**Material**")
        labs[1].markdown("**Size (req)**")
        labs[2].markdown("**Product Name**")
        labs[3].markdown("**Unit**")
        labs[4].markdown("**Qty**")
        labs[5].markdown("**Rate**")
        labs[6].markdown("**Amount**")

        for i, r in enumerate(rows):
            cols = st.columns([1.1, 1.1, 2, 0.9, 0.8, 0.9, 1.1])

            mat_current = (r.get("material") or "").strip()
            mat_options = DEFAULT_MATERIAL_OPTIONS.copy()
            if mat_current and mat_current not in mat_options:
                mat_options = [mat_current] + mat_options
            with cols[0]:
                st.selectbox("", options=mat_options,
                             index=mat_options.index(mat_current) if mat_current in mat_options else 0,
                             key=f"{session_key}_mat_{i}")

            with cols[1]:
                st.text_input("", value=r.get("size",""), key=f"{session_key}_size_{i}", placeholder="e.g., 600x600")

            with cols[2]:
                st.text_input("", value=r.get("product_name",""), key=f"{session_key}_name_{i}", placeholder="e.g., Renite")

            unit_current = (r.get("unit") or "").strip()
            unit_options = DEFAULT_UNIT_OPTIONS.copy()
            if unit_current and unit_current not in unit_options:
                unit_options = [unit_current] + unit_options
            with cols[3]:
                st.selectbox("", options=unit_options,
                             index=unit_options.index(unit_current) if unit_current in unit_options else 0,
                             key=f"{session_key}_unit_{i}")

            with cols[4]:
                st.text_input("", value=r.get("qty",""), key=f"{session_key}_qty_{i}", placeholder="")

            with cols[5]:
                st.text_input("", value=r.get("rate",""), key=f"{session_key}_rate_{i}", placeholder="")

            qty_widget_val = st.session_state.get(f"{session_key}_qty_{i}", r.get("qty",""))
            rate_widget_val = st.session_state.get(f"{session_key}_rate_{i}", r.get("rate",""))
            amt = _row_amount(qty_widget_val, rate_widget_val)
            with cols[6]:
                st.markdown(f"<div style='padding-top:6px;font-weight:600'>₹ {amt:,.2f}</div>", unsafe_allow_html=True)

        submitted = st.form_submit_button("Update Items")
        if submitted:
            subtotal = 0.0
            new_rows = []
            for i, _ in enumerate(rows):
                mat   = st.session_state.get(f"{session_key}_mat_{i}", "").strip()
                size  = st.session_state.get(f"{session_key}_size_{i}", "").strip()
                name  = st.session_state.get(f"{session_key}_name_{i}", "").strip()
                unit  = st.session_state.get(f"{session_key}_unit_{i}", "").strip()
                qty   = st.session_state.get(f"{session_key}_qty_{i}", "").strip()
                rate  = st.session_state.get(f"{session_key}_rate_{i}", "").strip()
                new_rows.append({
                    "material": mat,
                    "product_name": name,
                    "size": size,
                    "unit": unit,
                    "qty": qty,
                    "rate": rate
                })
                subtotal += _row_amount(qty, rate)
            st.session_state[session_key] = new_rows if new_rows else [
                {"material":"","product_name":"","size":"","unit":"","qty":"","rate":""} for _ in range(6)
            ]
            st.session_state[subtotal_key] = subtotal
            st.rerun()

    st.markdown(f"**Subtotal:** ₹ {st.session_state[subtotal_key]:,.2f}")
    return st.session_state[session_key], st.session_state[subtotal_key]

# --------- TABS ----------
tabs = st.tabs([
    "👥 Customers",
    "🏭 Suppliers",
    "📦 Purchase (Stock In)",
    "🧾 Sale (Stock Out)",
    "💵 Payments & Balances",
    "📊 Stock & Low Stock",
    "🗓️ Daily Report"
])

# ===================== Customers =====================
with tabs[0]:
    st.subheader("Add Customer (single)")
    cname = st.text_input("Customer Name*", key="cust_name", placeholder="e.g., Suresh Constructions")
    cphone = st.text_input("Phone", key="cust_phone", placeholder="e.g., 9876543210")
    caddr = st.text_area("Address", key="cust_addr", placeholder="Area / City / Notes")
    if st.button("Add Customer"):
        if not (cname or "").strip():
            st.error("Customer Name is required.")
        else:
            add_customer(cname, cphone, caddr)
            st.success("Customer added.")
            _schedule_reset("cust_name","cust_phone","cust_addr")
            st.rerun()

    st.divider()
    st.subheader("All Customers (with balance)")
    custs_df = customers_df()
    if not custs_df.empty:
        show = custs_df.copy()
        show["Balance (+due / −adv)"] = show["id"].astype(int).apply(customer_balance)
        st.dataframe(show[["id","name","phone","address","Balance (+due / −adv)"]], use_container_width=True)
    else:
        st.info("No customers yet.")

# ===================== Suppliers =====================
with tabs[1]:
    st.subheader("Add Supplier (single)")
    sname = st.text_input("Supplier Name*", key="sup_name", placeholder="e.g., ABC Ceramics")
    sphone = st.text_input("Phone", key="sup_phone", placeholder="e.g., 9876543210")
    saddr = st.text_area("Address", key="sup_addr", placeholder="Area / City / Notes")
    if st.button("Add Supplier"):
        if not (sname or "").strip():
            st.error("Supplier Name is required.")
        else:
            add_supplier(sname, sphone, saddr)
            st.success("Supplier added.")
            _schedule_reset("sup_name","sup_phone","sup_addr")
            st.rerun()

    st.divider()
    st.subheader("All Suppliers")
    sups_df = suppliers_df()
    if not sups_df.empty:
        st.dataframe(sups_df[["id","name","phone","address"]], use_container_width=True)
    else:
        st.info("No suppliers yet.")

# ===================== Purchase (IN) =====================
with tabs[2]:
    st.subheader("Record Purchase (single line)")
    prods = list_products()
    sups = list_suppliers()

    if not prods:
        st.info("No products yet — Quick Bill below can auto-create products.")
    else:
        prod_map = {f'{p["name"]} ({p.get("size") or ""} | {p["unit"]})': p for p in prods}
        choice = st.selectbox("Product*", list(prod_map.keys()), key="purchase_product")
        p = prod_map[choice]

        qty_text = st.text_input(f"Quantity ({p['unit']})*", key="purchase_qty", placeholder="")
        price_text = st.text_input("Price per unit (optional)", key="purchase_price", placeholder="")

        supplier_id = None
        if sups:
            sup_map = {s["name"]: s for s in sups}
            sel = st.selectbox("Supplier (optional)", ["-- none --"] + list(sup_map.keys()), key="purchase_supplier")
            if sel != "-- none --":
                supplier_id = int(sup_map[sel]["id"])

        notes = st.text_input("Notes", key="purchase_notes", placeholder="Bill no / supplier / remarks")

        amount = (float(qty_text or 0) * float(price_text or 0))
        st.markdown(f"**Amount:** ₹ {amount:,.2f}")

        if st.button("Save Purchase"):
            qty = float(qty_text or 0)
            price = float(price_text or 0)
            if qty <= 0:
                st.error("Quantity must be > 0.")
            else:
                ok = add_move("purchase", int(p["id"]), qty, price_per_unit=(price or None),
                              supplier_id=supplier_id, notes=notes or None)
                if ok:
                    st.success("Purchase saved.")
                else:
                    st.warning("Skipped duplicate purchase (same line recently saved).")
                _schedule_reset("purchase_qty","purchase_price","purchase_notes","purchase_supplier")
                st.rerun()

        st.caption(f"Current stock: **{product_stock(int(p['id']))} {p['unit']}**")

    # ---- Quick Bill (row form) ----
    st.divider()
    st.markdown("### 🧾 Quick Bill Entry — Purchase (multiple items)")
    bill_no_in = st.text_input("Bill / Invoice No. (optional)", key="bill_no_in")
    supplier_name = st.text_input("Supplier / Name (optional)", key="supplier_in")

    rows_in, subtotal_in = row_form("rows_purchase", "Items")
    if st.button("Save Purchase Bill", key="save_purchase_bill"):
        try:
            def first_non_blank(items, key, fallback):
                for r in items:
                    val = (r.get(key) or "").strip()
                    if val:
                        return val
                return fallback

            unit_default = first_non_blank(rows_in, "unit", "box")
            mat_default  = first_non_blank(rows_in, "material", "Tiles")
            supplier_id = ensure_supplier_by_name(supplier_name) if supplier_name else None

            saved = 0
            created_only = 0
            for ln in rows_in:
                name = (ln.get("product_name") or "").strip()
                size = (ln.get("size") or "").strip()
                if not name or not size:
                    continue
                qty  = float(ln.get("qty") or 0)
                rate = float(ln.get("rate") or 0)
                unit = (ln.get("unit") or unit_default).strip()
                material = (ln.get("material") or mat_default).strip()

                pid = ensure_product(name, size=size, unit=unit, material=material)

                if qty > 0:
                    note = f"Bill {bill_no_in}" if bill_no_in else None
                    if add_move("purchase", pid, qty, price_per_unit=(rate or None),
                                supplier_id=supplier_id, notes=(note or None)):
                        saved += 1
                else:
                    created_only += 1

            if saved or created_only:
                parts = []
                if saved: parts.append(f"Saved {saved} purchase line(s)")
                if created_only: parts.append(f"created {created_only} new product(s) at 0 stock")
                st.success(", ".join(parts) + ".")
                st.session_state["rows_purchase"] = [
                    {"material":"","product_name":"","size":"","unit":"","qty":"","rate":""} for _ in range(6)
                ]
                _schedule_reset("bill_no_in","supplier_in")
                st.rerun()
            else:
                st.warning("Nothing to save. Fill at least Product, Size and Qty.")
        except Exception as e:
            st.error(f"Error: {e}")

# ===================== Sale (OUT) =====================
with tabs[3]:
    st.subheader("Record Sale (single line)")
    prods = list_products()
    custs = list_customers()
    if not prods:
        st.info("No products yet — Quick Bill below can auto-create products.")
    else:
        prod_map = {f'{p["name"]} ({p.get("size") or ""} | {p["unit"]})': p for p in prods}
        choice = st.selectbox("Product*", list(prod_map.keys()), key="sale_product")
        p = prod_map[choice]
        stock_now = product_stock(int(p["id"]))

        qty_text = st.text_input(f"Quantity to sell ({p['unit']})*", key="sale_qty", placeholder="")
        price_text = st.text_input("Selling price per unit (optional)", key="sale_price", placeholder="")

        customer_id = None
        adv_now = 0.0
        if custs:
            cust_map = {c["name"]: c for c in custs}
            sel = st.selectbox("Customer (optional)", ["-- none --"] + list(cust_map.keys()), key="sale_customer")
            if sel != "-- none --":
                customer_id = int(cust_map[sel]["id"])
                prev_bal = customer_balance(customer_id)
                if prev_bal >= 0:
                    st.info(f"**Prev. balance for {sel}: ₹ {prev_bal:,.2f} (due)**")
                else:
                    st.success(f"**Advance available for {sel}: ₹ {abs(prev_bal):,.2f}**")
                adv_now = st.number_input("Advance received now (optional)", min_value=0.0, step=100.0, value=0.0, key="sale_adv")

        notes = st.text_input("Bill / Invoice No. or Notes", key="sale_notes", placeholder="Invoice no / remarks")
        line_total = float(qty_text or 0)*float(price_text or 0)
        st.markdown(f"<div class='amount'>Line Total: ₹ {line_total:,.2f}</div>", unsafe_allow_html=True)

        if customer_id:
            new_bal = customer_balance(customer_id) + line_total - float(adv_now or 0)
            st.caption(f"New balance after this line & advance: **₹ {new_bal:,.2f}**")

        if st.button("Save Sale"):
            qty = float(qty_text or 0)
            price = float(price_text or 0)
            if qty <= 0:
                st.error("Quantity must be > 0.")
            else:
                ok = add_move("sale", int(p["id"]), qty, price_per_unit=(price or None),
                              customer_id=customer_id, notes=notes or None)
                if ok:
                    if customer_id and float(adv_now or 0) > 0:
                        add_payment(customer_id, "payment", float(adv_now), notes=f"Advance for {notes}" if notes else "Advance")
                    st.success("Sale saved.")
                else:
                    st.warning("Skipped duplicate sale (same line recently saved).")
                _schedule_reset("sale_qty","sale_price","sale_notes","sale_customer","sale_adv")
                st.rerun()

        if stock_now < 0:
            st.caption(f"<span class='negative'>Current stock: {stock_now} {p['unit']} (negative)</span>", unsafe_allow_html=True)
        else:
            st.caption(f"Current stock: **{stock_now} {p['unit']}**")

    st.divider()
    st.markdown("### 🧾 Quick Bill Entry — Sale (multiple items)")
    bill_no_out = st.text_input("Bill / Invoice No. (optional)", key="bill_no_out")
    cust_out_name = st.text_input("Customer Name (optional)", key="customer_out")

    # Balance preview if existing customer typed
    cust_preview_id = None
    cdf = customers_df()
    if not cdf.empty and (cust_out_name or "").strip():
        row = cdf[cdf["name"].astype(str).str.lower() == cust_out_name.strip().lower()]
        if not row.empty:
            cust_preview_id = int(row.iloc[0]["id"])
            bal = customer_balance(cust_preview_id)
            if bal >= 0: st.info(f"Prev. balance: ₹ {bal:,.2f}")
            else:       st.success(f"Advance available: ₹ {abs(bal):,.2f}")

    bill_adv = st.number_input("Advance received now for this bill (optional)", min_value=0.0, step=100.0, value=0.0, key="sale_bill_adv")

    rows_out, subtotal_out = row_form("rows_sale", "Items")
    if cust_preview_id is not None:
        preview_new_bal = customer_balance(cust_preview_id) + float(subtotal_out or 0) - float(bill_adv or 0)
        st.caption(f"New balance after this bill & advance: **₹ {preview_new_bal:,.2f}**")

    if st.button("Save Sales Bill", key="save_sales_bill"):
        try:
            def first_non_blank(items, key, fallback):
                for r in items:
                    v = (r.get(key) or "").strip()
                    if v: return v
                return fallback

            unit_default = first_non_blank(rows_out, "unit", "box")
            mat_default  = first_non_blank(rows_out, "material", "Tiles")
            cust_id = ensure_customer_by_name(cust_out_name)

            saved = 0
            for ln in rows_out:
                name = (ln.get("product_name") or "").strip()
                size = (ln.get("size") or "").strip()
                if not name or not size:
                    continue
                qty  = float(ln.get("qty") or 0)
                rate = float(ln.get("rate") or 0)
                unit = (ln.get("unit") or unit_default).strip()
                material = (ln.get("material") or mat_default).strip()
                if qty <= 0:
                    continue
                pid = ensure_product(name, size=size, unit=unit, material=material)
                note = f"Bill {bill_no_out}" if bill_no_out else None
                if add_move("sale", pid, qty, price_per_unit=(rate or None),
                            customer_id=cust_id, notes=(note or None)):
                    saved += 1

            adv_msg = ""
            if cust_id and float(bill_adv or 0) > 0:
                if add_payment(cust_id, "payment", float(bill_adv),
                               notes=f"Advance for Bill {bill_no_out}" if bill_no_out else "Advance for bill"):
                    adv_msg = f" & recorded advance ₹{float(bill_adv):,.2f}"

            if saved:
                st.success(f"Saved {saved} sale line(s){adv_msg}.")
                st.session_state["rows_sale"] = [
                    {"material":"","product_name":"","size":"","unit":"","qty":"","rate":""} for _ in range(6)
                ]
                _schedule_reset("bill_no_out","customer_out","sale_bill_adv")
                st.rerun()
            else:
                st.warning("Nothing to save. Fill at least Product, Size and Qty > 0.")
        except Exception as e:
            st.error(f"Error: {e}")

# ===================== Payments & Balances =====================
with tabs[4]:
    st.subheader("Record Payment / Opening Due")
    custs = list_customers()
    if not custs:
        st.info("Add a customer first.")
    else:
        cmap = {c["name"]: c for c in custs}
        cname = st.selectbox("Customer", list(cmap.keys()), key="pay_cust")
        cid = int(cmap[cname]["id"])
        cur_bal = customer_balance(cid)
        if cur_bal >= 0:
            st.info(f"Current balance: ₹ {cur_bal:,.2f} (customer owes you)")
        else:
            st.success(f"Advance credit: ₹ {abs(cur_bal):,.2f}")

        mode = st.radio("What are you recording?", ["Payment received", "Opening due (+due)", "Advance (credit)"], horizontal=True)
        amt = st.number_input("Amount", min_value=0.0, step=100.0, value=0.0, key="pay_amt")
        note = st.text_input("Notes", key="pay_note")

        kind = {"Payment received":"payment", "Opening due (+due)":"opening_due", "Advance (credit)":"advance"}[mode]

        if st.button("Save"):
            if amt == 0:
                st.warning("Amount cannot be 0.")
            else:
                if add_payment(cid, kind, float(amt), notes=note):
                    nb = customer_balance(cid)
                    st.success(f"Saved. New balance: ₹ {nb:,.2f}" if nb >= 0 else f"Saved. Advance: ₹ {abs(nb):,.2f}")
                    _schedule_reset("pay_amt","pay_note"); st.rerun()
                else:
                    st.warning("Looks like a duplicate; nothing saved.")

    st.divider()
    st.subheader("Balances (Due or Advance)")
    cdf = customers_df()
    if not cdf.empty:
        rows = []
        for _, r in cdf.iterrows():
            cid = int(r["id"]); bal = customer_balance(cid)
            rows.append({"Customer": r["name"], "Phone": r["phone"], "Balance (+due / −adv)": bal})
        bal_df = pd.DataFrame(rows).sort_values("Customer")
        st.dataframe(bal_df, use_container_width=True)

    st.divider()
    st.subheader("Payments Ledger (All)")
    pays = payments_df()
    if not pays.empty:
        merged = pays.merge(cdf[["id","name"]], left_on="customer_id", right_on="id", how="left")
        merged = merged.rename(columns={"name":"Customer"})
        merged = merged.sort_values("ts")
        st.dataframe(merged[["ts","Customer","kind","amount","notes"]], use_container_width=True)
    else:
        st.info("No payments yet.")

# ===================== Stock & Low Stock =====================
with tabs[5]:
    st.subheader("Stock Levels")
    prods = list_products()
    if prods:
        df = pd.DataFrame(prods)
        df["current_stock"] = df["id"].astype(int).apply(product_stock)
        df["status"] = df["current_stock"].apply(lambda x: "NEGATIVE ⚠️" if x < 0 else "")
        low_thr = st.number_input("Low stock threshold (show items below this)", min_value=0.0, step=1.0, value=10.0)

        view = df[["name","material","size","unit","current_stock","status"]].sort_values(["size","name"], na_position="last")
        st.dataframe(view, use_container_width=True)

        low = df[df["current_stock"] < low_thr]
        st.markdown("#### ⚠️ Low Stock Items")
        if low.empty:
            st.success("All good. No low stock items.")
        else:
            st.dataframe(low[["name","size","unit","current_stock"]].sort_values(["size","name"], na_position="last"),
                         use_container_width=True)

        if st.button("Export Stock to CSV"):
            out = df[["name","material","size","unit","current_stock"]].copy().sort_values(["size","name"], na_position="last")
            out.to_csv("stock_export.csv", index=False)
            st.success("Saved as stock_export.csv (in the same folder).")
    else:
        st.info("No products yet.")

# ===================== Daily Report =====================
with tabs[6]:
    st.subheader("Daily Report (Sales, Purchases & Payments)")
    day = st.date_input("Pick a date", value=date.today())
    start = datetime(day.year, day.month, day.day, 0, 0, 0)
    end   = datetime(day.year, day.month, day.day, 23, 59, 59)

    moves = stock_moves_df()
    if not moves.empty:
        mv = moves.copy()
        mv["ts_dt"] = pd.to_datetime(mv["ts"], errors="coerce")
        mv = mv[(mv["ts_dt"] >= start) & (mv["ts_dt"] <= end)].sort_values("ts_dt")

        prods = products_df().rename(columns={"name":"product_name","size":"product_size"})
        custs = customers_df().rename(columns={"id":"cust_id","name":"customer_name"})
        sups  = suppliers_df().rename(columns={"id":"sup_id","name":"supplier_name"})

        rep = mv.merge(prods[["id","product_name","product_size","unit"]], left_on="product_id", right_on="id", how="left", suffixes=("","_p"))
        rep = rep.merge(custs[["cust_id","customer_name"]], left_on="customer_id", right_on="cust_id", how="left")
        rep = rep.merge(sups[["sup_id","supplier_name"]], left_on="supplier_id", right_on="sup_id", how="left")
        rep["time"] = rep["ts_dt"].dt.strftime("%H:%M")
        rep["qty_display"] = rep.apply(lambda r: f'{abs(r["qty"])} {r.get("unit","")}', axis=1)
        rep["value"] = rep.apply(lambda r: (abs(r["qty"]) * (r["price_per_unit"] or 0.0)), axis=1)
        rep["Party"] = rep.apply(lambda r: r["customer_name"] if r["kind"]=="sale" else r.get("supplier_name"), axis=1)

        st.markdown("#### Movements Today")
        show = rep[["time","kind","product_name","product_size","qty_display","Party","price_per_unit","value","notes"]]
        show = show.rename(columns={
            "kind":"Type","product_name":"Product","product_size":"Size",
            "price_per_unit":"Rate","value":"Amount","qty_display":"Qty"
        })
        st.dataframe(show, use_container_width=True)

        st.markdown("#### Bill-wise Totals (Notes)")
        by_bill = rep.groupby(["kind","notes"], dropna=False)["value"].sum().reset_index().rename(
            columns={"notes":"Bill / Notes","value":"Total Amount"}
        )
        by_bill["Bill / Notes"] = by_bill["Bill / Notes"].fillna("N/A")
        st.dataframe(by_bill.sort_values(["kind","Bill / Notes"]), use_container_width=True)

        sales = rep[rep["kind"]=="sale"].copy()
        if not sales.empty:
            st.markdown("#### Sales by Customer")
            cust = sales.groupby("Party", dropna=False)["value"].sum().reset_index().rename(
                columns={"Party":"Customer","value":"Total Amount"}
            )
            cust["Customer"] = cust["Customer"].fillna("N/A")
            st.dataframe(cust.sort_values("Customer"), use_container_width=True)

    # Payments today
    pays = payments_df()
    if not pays.empty:
        pp = pays.copy(); pp["ts_dt"] = pd.to_datetime(pp["ts"], errors="coerce")
        pp = pp[(pp["ts_dt"] >= start) & (pp["ts_dt"] <= end)]
        if not pp.empty:
            cdf = customers_df().rename(columns={"id":"cid"})
            pp = pp.merge(cdf[["cid","name"]], left_on="customer_id", right_on="cid", how="left")
            pp["time"] = pp["ts_dt"].dt.strftime("%H:%M")
            st.markdown("#### Payments / Advances Today")
            st.dataframe(pp[["time","name","kind","amount","notes"]].rename(columns={"name":"Customer","amount":"Amount"}), use_container_width=True)

    # End-of-day stock snapshot
    prods2 = list_products()
    snap = [{"Product": p["name"], "Size": p.get("size"), "Unit": p["unit"], "Stock Left": product_stock(int(p["id"])), "Status": "NEGATIVE ⚠️" if product_stock(int(p["id"])) < 0 else ""} for p in prods2]
    st.markdown("#### Stock Snapshot (End of Day)")
    st.dataframe(pd.DataFrame(snap).sort_values(["Size","Product"], na_position="last"), use_container_width=True)

st.divider()
st.caption("Quick Bill uses a form that won’t refresh while typing. Click **Update Items** to apply changes. Per-row Amount and a grand Subtotal are shown for clarity.")
st.caption("© 2023 Venkat Reddy. Inventory App for Tiles & Granite business.")
