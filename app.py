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


# ---------- UI ----------
st.set_page_config(page_title="Tiles & Granite Inventory", layout="wide")

st.markdown("""
<style>
html, body, [class*="css"]  { font-size: 18px !important; }
button, .stButton button { padding: 0.8rem 1.2rem !important; font-size: 18px !important; }
label { font-size: 18px !important; }
.negative { color: #b00020; font-weight: 700; }
.amount { font-weight: 700; }
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
    st.subheader("Quick Add Customers (multiple)")
    if "bulk_cust_df" not in st.session_state:
        st.session_state.bulk_cust_df = pd.DataFrame(
            [{"name":"","phone":"","address":""} for _ in range(5)]
        )
    edited_cust = st.data_editor(
        st.session_state.bulk_cust_df,
        use_container_width=True,
        num_rows="dynamic",
        key="bulk_customers_editor",
        column_config={
            "name": st.column_config.TextColumn("Customer Name (required)"),
            "phone": st.column_config.TextColumn("Phone"),
            "address": st.column_config.TextColumn("Address")
        }
    )
    st.session_state.bulk_cust_df = edited_cust

    if st.button("Save Customers"):
        added = 0
        for _, r in st.session_state.bulk_cust_df.fillna("").iterrows():
            nm = (r["name"] or "").strip()
            if not nm:
                continue
            add_customer(nm, str(r["phone"] or "").strip(), str(r["address"] or "").strip())
            added += 1
        if added:
            st.success(f"Added {added} customer(s).")
            st.session_state.bulk_cust_df = pd.DataFrame([{"name":"","phone":"","address":""} for _ in range(5)])
            st.rerun()
        else:
            st.info("Nothing to save. Enter at least one row with Customer Name.")

    st.divider()
    st.subheader("All Customers")
    custs = list_customers()
    if custs:
        st.dataframe(pd.DataFrame(custs)[["id","name","phone","address"]], use_container_width=True)
    else:
        st.info("No customers yet.")


# ===================== Purchase (IN) =====================
with tabs[1]:
    st.subheader("Record Purchase (Stock In)")
    prods = list_products()
    if not prods:
        st.info("No products yet — but products will be auto-created in the Quick Bill below.")
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

    # ---- Quick Bill Entry — Purchase (auto-create) ----
    st.divider()
    st.markdown("### 🧾 Quick Bill Entry — Purchase (multiple items)")

    if "qbe_purchase_df" not in st.session_state:
        # keep all editable columns as strings during editing (prevents dtype flips)
        st.session_state.qbe_purchase_df = pd.DataFrame(
            [{"product_name":"","size":"","qty":"","rate":"","unit":"","material":"","comments":""}
             for _ in range(8)], dtype="string"
        )

    bill_no_in = st.text_input("Bill / Invoice No. (optional)", key="bill_no_in")
    supplier_name = st.text_input("Supplier / Name (optional)", key="supplier_in")

    edited_in = st.data_editor(
        st.session_state.qbe_purchase_df,
        use_container_width=True,
        num_rows="dynamic",
        key="bill_editor_in",
        column_config={
            "product_name": st.column_config.TextColumn("Product Name"),
            "size": st.column_config.TextColumn("Size (required)"),
            "qty": st.column_config.TextColumn("Qty"),
            "rate": st.column_config.TextColumn("Rate"),
            # keep as TextColumn to avoid selectbox re-renders during typing
            "unit": st.column_config.TextColumn("Unit (pcs/boxs/sq_ft/bags/kgs)"),
            "material": st.column_config.TextColumn("Material (Tiles/Granites/Marble/Sanitary/CP/MYK)"),
            "comments": st.column_config.TextColumn("Comments")
        }
    )
    st.session_state.qbe_purchase_df = edited_in.astype("string")

    # display-only subtotal from a copy
    calc_in = st.session_state.qbe_purchase_df.copy()
    calc_in["q"] = pd.to_numeric(calc_in["qty"], errors="coerce").fillna(0.0)
    calc_in["r"] = pd.to_numeric(calc_in["rate"], errors="coerce").fillna(0.0)
    subtotal_in = float((calc_in["q"] * calc_in["r"]).sum())
    st.markdown(f"**Bill Subtotal (Purchase):** ₹ {subtotal_in:,.2f}")

    if st.button("Save Purchase Bill"):
        try:
            save_in = st.session_state.qbe_purchase_df.fillna("").astype(str).copy()

            # sensible defaults at SAVE time only
            def _first_non_blank(series, fallback):
                for v in series:
                    s = (str(v or "")).strip()
                    if s:
                        return s
                return fallback

            unit_default = _first_non_blank(save_in["unit"], "pcs")
            mat_default = _first_non_blank(save_in["material"], "Tiles")
            save_in.loc[save_in["unit"].str.strip() == "", "unit"] = unit_default
            save_in.loc[save_in["material"].str.strip() == "", "material"] = mat_default

            lines = save_in.to_dict(orient="records")
            supplier_id = ensure_customer_by_name(supplier_name) if supplier_name else None

            saved = 0
            for ln in lines:
                name = (ln.get("product_name") or "").strip()
                size = (ln.get("size") or "").strip()
                if not name or not size:
                    continue
                qty  = _to_float(ln.get("qty"))
                rate = _to_float(ln.get("rate"))
                unit = (ln.get("unit") or "pcs").strip()
                material = (ln.get("material") or "Tiles").strip()
                comments = (ln.get("comments") or "").strip()
                if qty <= 0:
                    continue
                pid = ensure_product(name, size=size, unit=unit, material=material)
                note = " | ".join([s for s in [f"Bill {bill_no_in}" if bill_no_in else None, comments or None] if s])
                add_move("purchase", pid, qty, price_per_unit=(rate or None),
                         customer_id=supplier_id, notes=(note or None))
                saved += 1

            if saved > 0:
                st.success(f"Saved {saved} purchase line(s).")
                st.session_state.qbe_purchase_df = pd.DataFrame(
                    [{"product_name":"","size":"","qty":"","rate":"","unit":"","material":"","comments":""}
                     for _ in range(8)], dtype="string"
                )
                for k in ["bill_no_in","supplier_in"]:
                    st.session_state[k] = ""
                st.rerun()
            else:
                st.warning("Nothing to save. Enter at least one row with Product Name, Size and Qty > 0.")
        except Exception as e:
            st.error(f"Could not save purchase bill: {e}")


# ===================== Sale (OUT) =====================
with tabs[2]:
    st.subheader("Record Sale (Stock Out)")
    prods = list_products()
    custs = list_customers()
    if not prods:
        st.info("No products yet — but products will be auto-created in the Quick Bill below.")
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
        else:
            st.caption("Tip: add customers in the Customers tab.")

        notes = st.text_input("Bill / Invoice No. or Notes", key="sale_notes", placeholder="Invoice no / payment mode / remarks")

        line_total = _to_float(qty_text) * _to_float(price_text)
        st.markdown(f"<div class='amount'>Line Total: ₹ {line_total:,.2f}</div>", unsafe_allow_html=True)

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

    # ---- Quick Bill Entry — Sale (auto-create) ----
    st.divider()
    st.markdown("### 🧾 Quick Bill Entry — Sale (multiple items)")

    if "qbe_sale_df" not in st.session_state:
        st.session_state.qbe_sale_df = pd.DataFrame(
            [{"product_name":"","size":"","qty":"","rate":"","unit":"","material":"","comments":""}
             for _ in range(8)], dtype="string"
        )

    bill_no_out = st.text_input("Bill / Invoice No. (optional)", key="bill_no_out")
    cust_out_name = st.text_input("Customer Name (optional)", key="customer_out")

    edited_out = st.data_editor(
        st.session_state.qbe_sale_df,
        use_container_width=True,
        num_rows="dynamic",
        key="bill_editor_out",
        column_config={
            "product_name": st.column_config.TextColumn("Product Name"),
            "size": st.column_config.TextColumn("Size (required)"),
            "qty": st.column_config.TextColumn("Qty"),
            "rate": st.column_config.TextColumn("Rate"),
            # text columns to avoid selectbox redraw quirks while typing
            "unit": st.column_config.TextColumn("Unit (pcs/boxs/sq_ft/bags/kgs)"),
            "material": st.column_config.TextColumn("Material (Tiles/Granites/Marble/Sanitary/CP/MYK)"),
            "comments": st.column_config.TextColumn("Comments")
        }
    )
    st.session_state.qbe_sale_df = edited_out.astype("string")

    # display-only subtotal from a copy
    calc_out = st.session_state.qbe_sale_df.copy()
    calc_out["q"] = pd.to_numeric(calc_out["qty"], errors="coerce").fillna(0.0)
    calc_out["r"] = pd.to_numeric(calc_out["rate"], errors="coerce").fillna(0.0)
    subtotal = float((calc_out["q"] * calc_out["r"]).sum())
    st.markdown(f"**Bill Subtotal:** ₹ {subtotal:,.2f}")

    if st.button("Save Sales Bill"):
        try:
            save_out = st.session_state.qbe_sale_df.fillna("").astype(str).copy()

            # defaults at SAVE time only (no in-edit mutations)
            def _first_non_blank(series, fallback):
                for v in series:
                    s = (str(v or "")).strip()
                    if s:
                        return s
                return fallback

            unit_default = _first_non_blank(save_out["unit"], "pcs")
            mat_default = _first_non_blank(save_out["material"], "Tiles")
            save_out.loc[save_out["unit"].str.strip() == "", "unit"] = unit_default
            save_out.loc[save_out["material"].str.strip() == "", "material"] = mat_default

            lines = save_out.to_dict(orient="records")
            cust_id = ensure_customer_by_name(cust_out_name)

            saved = 0
            for ln in lines:
                name = (ln.get("product_name") or "").strip()
                size = (ln.get("size") or "").strip()
                if not name or not size:
                    continue
                qty  = _to_float(ln.get("qty"))
                rate = _to_float(ln.get("rate"))
                unit = (ln.get("unit") or "pcs").strip()
                material = (ln.get("material") or "Tiles").strip()
                comments = (ln.get("comments") or "").strip()
                if qty <= 0:
                    continue
                pid = ensure_product(name, size=size, unit=unit, material=material)
                note = " | ".join([s for s in [f"Bill {bill_no_out}" if bill_no_out else None, comments or None] if s])
                add_move("sale", pid, qty, price_per_unit=(rate or None),
                         customer_id=cust_id, notes=(note or None))
                saved += 1

            if saved > 0:
                st.success(f"Saved {saved} sale line(s).")
                st.session_state.qbe_sale_df = pd.DataFrame(
                    [{"product_name":"","size":"","qty":"","rate":"","unit":"","material":"","comments":""}
                     for _ in range(8)], dtype="string"
                )
                for k in ["bill_no_out","customer_out"]:
                    st.session_state[k] = ""
                st.rerun()
            else:
                st.warning("Nothing to save. Enter at least one row with Product Name, Size and Qty > 0.")
        except Exception as e:
            st.error(f"Could not save bill: {e}")


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
st.caption("Tip: In Quick Bill, cells never get changed while you type. Defaults for Unit/Material apply only when you save.")