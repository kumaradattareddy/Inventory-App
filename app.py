import sqlite3
from contextlib import closing
from datetime import date, datetime
import pandas as pd
import streamlit as st
import hashlib, secrets

DB_PATH = "inventory.db"

# ======= SINGLE-USER CONFIG (must be lowercase) =======
ALLOWED_USERS = {"venkat reddy"}     # only these usernames can login
DEFAULT_USERNAME = "venkat reddy"    # must be in ALLOWED_USERS
DEFAULT_PASSWORD = "1234"            # change after first login
# ======================================================

# ---------- AUTH ----------
def _hash_password(password: str, salt: str) -> str:
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), bytes.fromhex(salt), 100_000)
    return dk.hex()

def user_exists(username: str) -> bool:
    rows = run_query("SELECT 1 FROM users WHERE username=?", (username.strip().lower(),), fetch=True)
    return bool(rows)

def create_user(username: str, password: str):
    username = username.strip().lower()
    salt = secrets.token_hex(16)
    pwd_hash = _hash_password(password, salt)
    run_query("INSERT INTO users(username, password_hash, salt) VALUES(?,?,?)",
              (username, pwd_hash, salt))

def verify_login(username: str, password: str):
    username = username.strip().lower()
    if username not in ALLOWED_USERS:
        return None
    row = run_query("SELECT username, password_hash, salt FROM users WHERE username=?",
                    (username,), fetch=True)
    if not row:
        return None
    row = dict(row[0])
    if _hash_password(password, row["salt"]) == row["password_hash"]:
        return {"username": row["username"]}
    return None

# ---------- DB ----------
def init_db():
    with closing(sqlite3.connect(DB_PATH)) as conn:
        c = conn.cursor()
        c.execute("""
        CREATE TABLE IF NOT EXISTS products(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            material TEXT,
            size TEXT,
            unit TEXT NOT NULL,
            opening_stock REAL DEFAULT 0
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS customers(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            phone TEXT,
            address TEXT
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS stock_moves(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            kind TEXT NOT NULL,
            product_id INTEGER NOT NULL,
            qty REAL NOT NULL,
            price_per_unit REAL,
            customer_id INTEGER,
            notes TEXT,
            FOREIGN KEY(product_id) REFERENCES products(id),
            FOREIGN KEY(customer_id) REFERENCES customers(id)
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS users(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            salt TEXT NOT NULL
        )""")
        conn.commit()

def run_query(sql, params=(), fetch=False):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(sql, params)
        if fetch:
            return c.fetchall()
        conn.commit()

# ---------- HELPERS ----------
def list_products():
    rows = run_query("SELECT * FROM products ORDER BY name", fetch=True)
    return [dict(r) for r in rows]

def list_customers():
    rows = run_query("SELECT * FROM customers ORDER BY name", fetch=True)
    return [dict(r) for r in rows]

def product_stock(product_id):
    row = run_query("SELECT opening_stock FROM products WHERE id=?", (product_id,), fetch=True)
    if not row:
        return 0.0
    opening = row[0]["opening_stock"] or 0.0
    moves = run_query("SELECT COALESCE(SUM(qty),0) AS s FROM stock_moves WHERE product_id=?",
                      (product_id,), fetch=True)
    return float(opening) + float(moves[0]["s"])

def add_product(name, material, size, unit, opening_stock):
    run_query(
        "INSERT INTO products(name,material,size,unit,opening_stock) VALUES(?,?,?,?,?)",
        (name.strip(), material.strip() if material else None,
         size.strip() if size else None, unit, opening_stock)
    )

def add_customer(name, phone, address):
    run_query(
        "INSERT INTO customers(name,phone,address) VALUES(?,?,?)",
        (name.strip(), phone.strip() if phone else None, address.strip() if address else None)
    )

def add_move(kind, product_id, qty, price_per_unit=None, customer_id=None, notes=None, when=None):
    ts = (when or datetime.now()).isoformat(timespec="seconds")
    if kind == "sale" and qty > 0:
        qty = -qty
    run_query(
        "INSERT INTO stock_moves(ts,kind,product_id,qty,price_per_unit,customer_id,notes) VALUES(?,?,?,?,?,?,?)",
        (ts, kind, product_id, qty, price_per_unit, customer_id, notes)
    )

def moves_on_day(d: date):
    start = datetime(d.year, d.month, d.day, 0, 0, 0).isoformat(timespec="seconds")
    end   = datetime(d.year, d.month, d.day, 23, 59, 59).isoformat(timespec="seconds")
    rows = run_query("""
        SELECT m.*, p.name AS product_name, p.unit, c.name AS customer_name
        FROM stock_moves m
        JOIN products p ON p.id = m.product_id
        LEFT JOIN customers c ON c.id = m.customer_id
        WHERE m.ts BETWEEN ? AND ?
        ORDER BY m.ts
    """, (start, end), fetch=True)
    return [dict(r) for r in rows]

def get_product_by_name_size_unit(name: str, size: str, unit: str):
    rows = run_query(
        "SELECT * FROM products WHERE lower(name)=? AND lower(COALESCE(size,''))=? AND unit=?",
        (name.strip().lower(), (size or "").strip().lower(), unit), fetch=True
    )
    return dict(rows[0]) if rows else None

def ensure_product(name: str, size: str, unit: str, material: str = None, opening_stock: float = 0.0):
    p = get_product_by_name_size_unit(name, size, unit)
    if p:
        return p["id"]
    add_product(name=name, material=material, size=size, unit=unit, opening_stock=opening_stock)
    p = get_product_by_name_size_unit(name, size, unit)
    return p["id"]

def ensure_customer_by_name(name: str, phone: str = None, address: str = None):
    name = (name or "").strip()
    if not name:
        return None
    rows = run_query("SELECT * FROM customers WHERE lower(name)=?", (name.lower(),), fetch=True)
    if rows:
        return dict(rows[0])["id"]
    add_customer(name, phone, address)
    rows = run_query("SELECT * FROM customers WHERE lower(name)=?", (name.lower(),), fetch=True)
    return dict(rows[0])["id"]

def _to_float(txt: str) -> float:
    s = (txt or "").strip()
    if not s:
        return 0.0
    try:
        return float(s)
    except:
        return 0.0

# ---------- ROW FORM (improved & fixed) ----------
def ensure_rows(session_key: str, start_rows: int = 6):
    if session_key not in st.session_state:
        st.session_state[session_key] = [
            {"product_name":"","size":"","qty":"","rate":"","unit":"","material":"","comments":""}
            for _ in range(start_rows)
        ]

def _row_amount(qty_txt: str, rate_txt: str) -> float:
    return _to_float(qty_txt) * _to_float(rate_txt)

def row_form(session_key: str, title: str):
    """
    Row editor:
    - inputs are wrapped in a form so typing doesn't rerun
    - deletion is done via checkboxes processed on submit
    - amounts/subtotal are recalculated on submit and shown
    """
    ensure_rows(session_key)
    rows = st.session_state[session_key]

    st.markdown(f"#### {title}")

    # top-level controls (don't force rerun while typing)
    c1, c2, c3 = st.columns([1,1,6])
    with c1:
        if st.button("➕ Add row", key=f"add_{session_key}"):
            rows.append({"product_name":"","size":"","qty":"","rate":"","unit":"","material":"","comments":""})
            st.rerun()
    with c2:
        if st.button("🧹 Clear", key=f"clear_{session_key}"):
            st.session_state[session_key] = [
                {"product_name":"","size":"","qty":"","rate":"","unit":"","material":"","comments":""}
                for _ in range(6)
            ]
            st.rerun()
    st.caption("Tip: type freely; the table won’t refresh until you click **Update Items**.")

    subtotal_key = f"{session_key}_subtotal"
    if subtotal_key not in st.session_state:
        st.session_state[subtotal_key] = 0.0

    # form to avoid reruns while typing
    with st.form(f"form_{session_key}", clear_on_submit=False):
        # labels
        lab = st.columns([2.0,1.3,0.9,1.0,1.0,1.3,2.0,1.0,0.5])
        lab[0].markdown("**Product Name**")
        lab[1].markdown("**Size (req)**")
        lab[2].markdown("**Qty**")
        lab[3].markdown("**Rate**")
        lab[4].markdown("**Unit**")
        lab[5].markdown("**Material**")
        lab[6].markdown("**Comments**")
        lab[7].markdown("**Amount**")
        lab[8].markdown("**Del**")

        # per-row widgets
        for i, r in enumerate(rows):
            cols = st.columns([2.0,1.3,0.9,1.0,1.0,1.3,2.0,1.0,0.5])
            with cols[0]:
                st.text_input("", value=r["product_name"], key=f"{session_key}_name_{i}", placeholder="e.g., Renite")
            with cols[1]:
                st.text_input("", value=r["size"], key=f"{session_key}_size_{i}", placeholder="e.g., 600x600")
            with cols[2]:
                st.text_input("", value=r["qty"], key=f"{session_key}_qty_{i}", placeholder="")
            with cols[3]:
                st.text_input("", value=r["rate"], key=f"{session_key}_rate_{i}", placeholder="")
            with cols[4]:
                st.text_input("", value=r["unit"], key=f"{session_key}_unit_{i}", placeholder="pcs/boxes/sq_ft/bags/kgs")
            with cols[5]:
                st.text_input("", value=r["material"], key=f"{session_key}_mat_{i}", placeholder="Tiles/Granites/Marble/…")
            with cols[6]:
                st.text_input("", value=r["comments"], key=f"{session_key}_cmt_{i}", placeholder="")
            # Show amount based on the latest widget values if available (after submit/rerun)
            qty_widget_val = st.session_state.get(f"{session_key}_qty_{i}", r.get("qty",""))
            rate_widget_val = st.session_state.get(f"{session_key}_rate_{i}", r.get("rate",""))
            amt = _row_amount(qty_widget_val, rate_widget_val)
            with cols[7]:
                st.markdown(f"<div style='padding-top:6px;font-weight:600'>₹ {amt:,.2f}</div>", unsafe_allow_html=True)
            with cols[8]:
                st.checkbox("", value=False, key=f"{session_key}_del_{i}")

        submitted = st.form_submit_button("Update Items")

        if submitted:
            new_rows = []
            subtotal = 0.0
            for i, _r in enumerate(rows):
                name = st.session_state.get(f"{session_key}_name_{i}", "")
                size = st.session_state.get(f"{session_key}_size_{i}", "")
                qty  = st.session_state.get(f"{session_key}_qty_{i}", "")
                rate = st.session_state.get(f"{session_key}_rate_{i}", "")
                unit = st.session_state.get(f"{session_key}_unit_{i}", "")
                mat  = st.session_state.get(f"{session_key}_mat_{i}", "")
                cmt  = st.session_state.get(f"{session_key}_cmt_{i}", "")
                mark_del = st.session_state.get(f"{session_key}_del_{i}", False)

                if not mark_del:
                    new_rows.append({
                        "product_name": name,
                        "size": size,
                        "qty": qty,
                        "rate": rate,
                        "unit": unit,
                        "material": mat,
                        "comments": cmt
                    })

                subtotal += _to_float(qty) * _to_float(rate)

            st.session_state[session_key] = new_rows if new_rows else [
                {"product_name":"","size":"","qty":"","rate":"","unit":"","material":"","comments":""}
                for _ in range(6)
            ]
            st.session_state[subtotal_key] = subtotal

            # Use st.rerun() (works across streamlit versions consistently in this app)
            st.rerun()

    st.markdown(f"**Subtotal:** ₹ {st.session_state[subtotal_key]:,.2f}")
    return st.session_state[session_key], st.session_state[subtotal_key]

# ---------- UI ----------
st.set_page_config(page_title="Tiles & Granite Inventory", layout="wide")

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

# Init DB and ensure default user exists
init_db()
if DEFAULT_USERNAME in ALLOWED_USERS and not user_exists(DEFAULT_USERNAME):
    try:
        create_user(DEFAULT_USERNAME, DEFAULT_PASSWORD)
    except Exception:
        pass

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

# --------- TABS ----------
tabs = st.tabs([
    "👥 Customers",
    "📦 Purchase (Stock In)",
    "🧾 Sale (Stock Out)",
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
            for k in ["cust_name","cust_phone","cust_addr"]:
                st.session_state[k] = ""
            st.rerun()

    st.divider()
    st.subheader("All Customers")
    custs = list_customers()
    if custs:
        st.dataframe(pd.DataFrame(custs)[["id","name","phone","address"]], use_container_width=True)
    else:
        st.info("No customers yet.")

# ===================== Purchase (IN) =====================
with tabs[1]:
    st.subheader("Record Purchase (single line)")
    prods = list_products()
    if not prods:
        st.info("No products yet — Quick Bill below can auto-create products.")
    else:
        prod_map = {f'{p["name"]} ({p["size"] or ""} | {p["unit"]})': p for p in prods}
        choice = st.selectbox("Product*", list(prod_map.keys()), key="purchase_product")
        p = prod_map[choice]

        qty_text = st.text_input(f"Quantity ({p['unit']})*", key="purchase_qty", placeholder="")
        price_text = st.text_input("Price per unit (optional)", key="purchase_price", placeholder="")
        notes = st.text_input("Notes", key="purchase_notes", placeholder="Bill no / supplier / remarks")

        amount = _to_float(qty_text) * _to_float(price_text)
        st.markdown(f"**Amount:** ₹ {amount:,.2f}")

        if st.button("Save Purchase"):
            qty = _to_float(qty_text)
            price = _to_float(price_text)
            if qty <= 0:
                st.error("Quantity must be > 0.")
            else:
                add_move("purchase", p["id"], qty, price_per_unit=(price or None), notes=notes or None)
                st.success("Purchase saved.")
                for k in ["purchase_qty","purchase_price","purchase_notes"]:
                    st.session_state[k] = ""
                st.rerun()

        st.caption(f"Current stock: **{product_stock(p['id'])} {p['unit']}**")

    # ---- Quick Bill (row form) ----
    st.divider()
    st.markdown("### 🧾 Quick Bill Entry — Purchase (multiple items)")
    bill_no_in = st.text_input("Bill / Invoice No. (optional)", key="bill_no_in")
    supplier_name = st.text_input("Supplier / Name (optional)", key="supplier_in")

    rows_in, subtotal_in = row_form("rows_purchase", "Items")
    if st.button("Save Purchase Bill", key="save_purchase_bill"):
        try:
            # decide defaults only now
            def first_non_blank(items, key, fallback):
                for r in items:
                    val = (r.get(key) or "").strip()
                    if val:
                        return val
                return fallback

            unit_default = first_non_blank(rows_in, "unit", "pcs")
            mat_default  = first_non_blank(rows_in, "material", "Tiles")

            supplier_id = ensure_customer_by_name(supplier_name) if supplier_name else None
            saved = 0
            for ln in rows_in:
                name = (ln.get("product_name") or "").strip()
                size = (ln.get("size") or "").strip()
                if not name or not size:
                    continue
                qty  = _to_float(ln.get("qty"))
                rate = _to_float(ln.get("rate"))
                unit = (ln.get("unit") or unit_default).strip()
                material = (ln.get("material") or mat_default).strip()
                comments = (ln.get("comments") or "").strip()
                if qty <= 0:
                    continue
                pid = ensure_product(name, size=size, unit=unit, material=material)
                note = " | ".join([s for s in [f"Bill {bill_no_in}" if bill_no_in else None, comments or None] if s])
                add_move("purchase", pid, qty, price_per_unit=(rate or None),
                         customer_id=supplier_id, notes=(note or None))
                saved += 1

            if saved:
                st.success(f"Saved {saved} purchase line(s).")
                st.session_state["rows_purchase"] = [
                    {"product_name":"","size":"","qty":"","rate":"","unit":"","material":"","comments":""}
                    for _ in range(6)
                ]
                for k in ["bill_no_in","supplier_in"]:
                    st.session_state[k] = ""
                st.rerun()
            else:
                st.warning("Nothing to save. Fill at least Product, Size and Qty > 0.")
        except Exception as e:
            st.error(f"Error: {e}")

# ===================== Sale (OUT) =====================
with tabs[2]:
    st.subheader("Record Sale (single line)")
    prods = list_products()
    custs = list_customers()
    if not prods:
        st.info("No products yet — Quick Bill below can auto-create products.")
    else:
        prod_map = {f'{p["name"]} ({p["size"] or ""} | {p["unit"]})': p for p in prods}
        choice = st.selectbox("Product*", list(prod_map.keys()), key="sale_product")
        p = prod_map[choice]
        stock_now = product_stock(p["id"])

        qty_text = st.text_input(f"Quantity to sell ({p['unit']})*", key="sale_qty", placeholder="")
        price_text = st.text_input("Selling price per unit (optional)", key="sale_price", placeholder="")

        customer_id = None
        if custs:
            cust_map = {c["name"]: c for c in custs}
            sel = st.selectbox("Customer (optional)", ["-- none --"] + list(cust_map.keys()), key="sale_customer")
            if sel != "-- none --":
                customer_id = cust_map[sel]["id"]
        notes = st.text_input("Bill / Invoice No. or Notes", key="sale_notes", placeholder="Invoice no / remarks")
        st.markdown(f"<div class='amount'>Line Total: ₹ {_to_float(qty_text)*_to_float(price_text):,.2f}</div>", unsafe_allow_html=True)

        if st.button("Save Sale"):
            qty = _to_float(qty_text)
            price = _to_float(price_text)
            if qty <= 0:
                st.error("Quantity must be > 0.")
            else:
                add_move("sale", p["id"], qty, price_per_unit=(price or None),
                         customer_id=customer_id, notes=notes or None)
                st.success("Sale saved.")
                for k in ["sale_qty","sale_price","sale_notes","sale_customer"]:
                    st.session_state[k] = ""
                st.rerun()

        if stock_now < 0:
            st.caption(f"<span class='negative'>Current stock: {stock_now} {p['unit']} (negative)</span>", unsafe_allow_html=True)
        else:
            st.caption(f"Current stock: **{stock_now} {p['unit']}**")

    st.divider()
    st.markdown("### 🧾 Quick Bill Entry — Sale (multiple items)")
    bill_no_out = st.text_input("Bill / Invoice No. (optional)", key="bill_no_out")
    cust_out_name = st.text_input("Customer Name (optional)", key="customer_out")

    rows_out, subtotal_out = row_form("rows_sale", "Items")
    if st.button("Save Sales Bill", key="save_sales_bill"):
        try:
            def first_non_blank(items, key, fallback):
                for r in items:
                    val = (r.get(key) or "").strip()
                    if val:
                        return val
                return fallback

            unit_default = first_non_blank(rows_out, "unit", "pcs")
            mat_default  = first_non_blank(rows_out, "material", "Tiles")
            cust_id = ensure_customer_by_name(cust_out_name)

            saved = 0
            for ln in rows_out:
                name = (ln.get("product_name") or "").strip()
                size = (ln.get("size") or "").strip()
                if not name or not size:
                    continue
                qty  = _to_float(ln.get("qty"))
                rate = _to_float(ln.get("rate"))
                unit = (ln.get("unit") or unit_default).strip()
                material = (ln.get("material") or mat_default).strip()
                comments = (ln.get("comments") or "").strip()
                if qty <= 0:
                    continue
                pid = ensure_product(name, size=size, unit=unit, material=material)
                note = " | ".join([s for s in [f"Bill {bill_no_out}" if bill_no_out else None, comments or None] if s])
                add_move("sale", pid, qty, price_per_unit=(rate or None),
                         customer_id=cust_id, notes=(note or None))
                saved += 1

            if saved:
                st.success(f"Saved {saved} sale line(s).")
                st.session_state["rows_sale"] = [
                    {"product_name":"","size":"","qty":"","rate":"","unit":"","material":"","comments":""}
                    for _ in range(6)
                ]
                for k in ["bill_no_out","customer_out"]:
                    st.session_state[k] = ""
                st.rerun()
            else:
                st.warning("Nothing to save. Fill at least Product, Size and Qty > 0.")
        except Exception as e:
            st.error(f"Error: {e}")

# ===================== Stock & Low Stock =====================
with tabs[3]:
    st.subheader("Stock Levels")
    prods = list_products()
    if prods:
        df = pd.DataFrame(prods)
        df["current_stock"] = df["id"].apply(product_stock)
        df["status"] = df["current_stock"].apply(lambda x: "NEGATIVE ⚠️" if x < 0 else "")
        low_thr = st.number_input("Low stock threshold (show items below this)", min_value=0.0, step=1.0, value=10.0)
        view = df[["id","name","material","size","unit","current_stock","status"]].sort_values("name")
        st.dataframe(view, use_container_width=True)

        low = df[df["current_stock"] < low_thr]
        st.markdown("#### ⚠️ Low Stock Items")
        if low.empty:
            st.success("All good. No low stock items.")
        else:
            st.dataframe(low[["id","name","size","unit","current_stock"]], use_container_width=True)

        if st.button("Export Stock to CSV"):
            out = df[["name","material","size","unit","current_stock"]].copy()
            out.to_csv("stock_export.csv", index=False)
            st.success("Saved as stock_export.csv (in the same folder).")
    else:
        st.info("No products yet.")

# ===================== Daily Report =====================
with tabs[4]:
    st.subheader("Daily Report (Sales & Purchases)")
    day = st.date_input("Pick a date", value=date.today())
    rows = moves_on_day(day)
    if rows:
        rep = pd.DataFrame(rows)
        rep["time"] = pd.to_datetime(rep["ts"]).dt.strftime("%H:%M")
        rep["qty_display"] = rep.apply(lambda r: f'{abs(r["qty"])} {r["unit"]}', axis=1)
        rep["value"] = rep.apply(lambda r: (abs(r["qty"]) * (r["price_per_unit"] or 0.0)), axis=1)

        st.markdown("#### All Movements Today")
        show = rep[["time","kind","product_name","qty_display","customer_name","price_per_unit","value","notes"]]
        show = show.rename(columns={
            "kind":"Type","product_name":"Product","customer_name":"Customer",
            "price_per_unit":"Rate","value":"Amount","qty_display":"Qty"
        })
        st.dataframe(show, use_container_width=True)

        sales = rep[rep["kind"]=="sale"].copy()
        if not sales.empty:
            st.markdown("#### Who bought today (Sales by Customer)")
            cust = sales.groupby("customer_name", dropna=False)["value"].sum().reset_index().rename(
                columns={"customer_name":"Customer","value":"Total Amount"}
            )
            cust["Customer"] = cust["Customer"].fillna("N/A")
            st.dataframe(cust, use_container_width=True)

        st.markdown("#### Stock Snapshot (End of Day)")
        prods = list_products()
        snap = []
        for p in prods:
            qty_left = product_stock(p["id"])
            snap.append({
                "Product": p["name"], "Size": p["size"], "Unit": p["unit"],
                "Stock Left": qty_left, "Status": "NEGATIVE ⚠️" if qty_left < 0 else ""
            })
        st.dataframe(pd.DataFrame(snap).sort_values("Product"), use_container_width=True)

        if st.button("Export Today’s Report to CSV"):
            show.to_csv(f"report_{day.isoformat()}.csv", index=False)
            st.success(f"Saved as report_{day.isoformat()}.csv")
    else:
        st.info("No entries on this day yet.")

st.divider()
st.caption("Quick Bill uses a form that won’t refresh while typing. Click **Update Items** to apply changes. Per-row Amount and a grand Subtotal are shown for clarity.")
