import sqlite3
from contextlib import closing
from datetime import date, datetime
import pandas as pd
import streamlit as st
import hashlib, secrets

DB_PATH = "inventory.db"

# ======= SINGLE-USER CONFIG =======
ALLOWED_USERS = {"grandpa"}          # <- only these usernames can login
DEFAULT_USERNAME = "grandpa"         # <- created automatically if missing
DEFAULT_PASSWORD = "1234"            # <- change this before giving it to him
# ==================================

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
    run_query(
        "INSERT INTO users(username, password_hash, salt) VALUES(?,?,?)",
        (username, pwd_hash, salt)
    )

def verify_login(username: str, password: str):
    username = username.strip().lower()
    # enforce allowlist first
    if username not in ALLOWED_USERS:
        return None
    row = run_query("SELECT username, password_hash, salt FROM users WHERE username=?", (username,), fetch=True)
    if not row:
        return None
    row = dict(row[0])
    if _hash_password(password, row["salt"]) == row["password_hash"]:
        return {"username": row["username"]}
    return None

def change_password(username: str, new_password: str):
    salt = secrets.token_hex(16)
    pwd_hash = _hash_password(new_password, salt)
    run_query("UPDATE users SET password_hash=?, salt=? WHERE username=?", (pwd_hash, salt, username.strip().lower()))

# ---------- DB SETUP ----------
def init_db():
    with closing(sqlite3.connect(DB_PATH)) as conn:
        c = conn.cursor()
        c.execute("""
        CREATE TABLE IF NOT EXISTS products(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            material TEXT,                 -- Tiles / Granites / Marble / Sanitary / CP / MYK / Other
            size TEXT,                     -- e.g., 2x2 ft, 600x600 mm
            unit TEXT NOT NULL,            -- pcs / box / sq_ft / sq_m / bags / kgs
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
            ts TEXT NOT NULL,              -- ISO datetime
            kind TEXT NOT NULL,            -- purchase / sale / adjust
            product_id INTEGER NOT NULL,
            qty REAL NOT NULL,
            price_per_unit REAL,           -- optional
            customer_id INTEGER,           -- for sales
            notes TEXT,
            FOREIGN KEY(product_id) REFERENCES products(id),
            FOREIGN KEY(customer_id) REFERENCES customers(id)
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
    # opening + sum(moves)
    row = run_query("SELECT opening_stock FROM products WHERE id=?", (product_id,), fetch=True)
    if not row:
        return 0.0
    opening = row[0]["opening_stock"] or 0.0
    moves = run_query("SELECT COALESCE(SUM(qty),0) AS s FROM stock_moves WHERE product_id=?", (product_id,), fetch=True)
    return float(opening) + float(moves[0]["s"])

def add_product(name, material, size, unit, opening_stock):
    run_query(
        "INSERT INTO products(name,material,size,unit,opening_stock) VALUES(?,?,?,?,?)",
        (name.strip(), material.strip() if material else None, size.strip() if size else None, unit, opening_stock)
    )

def add_customer(name, phone, address):
    run_query(
        "INSERT INTO customers(name,phone,address) VALUES(?,?,?)",
        (name.strip(), phone.strip() if phone else None, address.strip() if address else None)
    )

def add_move(kind, product_id, qty, price_per_unit=None, customer_id=None, notes=None, when=None):
    ts = (when or datetime.now()).isoformat(timespec="seconds")
    # For sales, qty should be negative (stock goes down)
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

# ---------- UI ----------
st.set_page_config(page_title="Tiles & Granite Inventory", layout="wide")

# Make fonts/buttons a bit larger for seniors
st.markdown("""
<style>
html, body, [class*="css"]  {
  font-size: 18px !important;
}
button, .stButton button {
  padding: 0.8rem 1.2rem !important;
  font-size: 18px !important;
}
label { font-size: 18px !important; }
.negative { color: #b00020; font-weight: 700; }
</style>
""", unsafe_allow_html=True)

init_db()

st.title("Tiles & Granite Inventory")

tabs = st.tabs([
    "➕ Add Products",
    "👥 Customers",
    "📦 Purchase (Stock In)",
    "🧾 Sale (Stock Out)",
    "📊 Stock & Low Stock",
    "🗓️ Daily Report"
])

# --- Add Products ---
with tabs[0]:
    st.subheader("Add a New Product")
    colA, colB = st.columns(2)
    with colA:
        name = st.text_input("Product Name*", placeholder="e.g., Kajaria 2x2 Polished")
        material = st.selectbox(
            "Material",
            ["Tiles", "Granites", "Marble", "Sanitary", "CP", "MYK", "Other"],
            index=0
        )
        size = st.text_input("Size", placeholder="e.g., 2x2 ft / 600x600 mm")
    with colB:
        unit = st.selectbox(
            "Unit*",
            ["pcs", "box", "sq_ft", "sq_m", "bags", "kgs"],
            index=0
        )
        opening = st.number_input("Opening Stock", min_value=0.0, step=1.0, value=0.0, help="Initial quantity you already have.")
    if st.button("Add Product"):
        if not name.strip():
            st.error("Product Name is required.")
        else:
            add_product(name, material, size, unit, opening)
            st.success("Product added.")
    st.divider()
    st.subheader("All Products")
    prods = list_products()
    if prods:
        df = pd.DataFrame(prods)
        df["current_stock"] = df["id"].apply(product_stock)
        st.dataframe(df[["id","name","material","size","unit","opening_stock","current_stock"]], use_container_width=True)
    else:
        st.info("No products yet.")

# --- Customers ---
with tabs[1]:
    st.subheader("Add Customer")
    col1, col2 = st.columns(2)
    with col1:
        cname = st.text_input("Customer Name*", placeholder="e.g., Suresh Constructions")
        cphone = st.text_input("Phone", placeholder="e.g., 9876543210")
    with col2:
        caddr = st.text_area("Address", placeholder="Area / City / Notes")
    if st.button("Add Customer"):
        if not cname.strip():
            st.error("Customer Name is required.")
        else:
            add_customer(cname, cphone, caddr)
            st.success("Customer added.")
    st.divider()
    st.subheader("All Customers")
    custs = list_customers()
    if custs:
        st.dataframe(pd.DataFrame(custs)[["id","name","phone","address"]], use_container_width=True)
    else:
        st.info("No customers yet.")

# --- Purchase (IN) ---
with tabs[2]:
    st.subheader("Record Purchase (Stock In)")
    prods = list_products()
    if not prods:
        st.warning("Add a product first in 'Add Products'.")
    else:
        prod_map = {f'{p["name"]} ({p["size"] or ""} | {p["unit"]})': p for p in prods}
        choice = st.selectbox("Product*", list(prod_map.keys()), key="purchase_product")
        p = prod_map[choice]
        col1, col2, col3 = st.columns(3)
        with col1:
            qty = st.number_input(f"Quantity ({p['unit']})*", min_value=0.0, step=1.0, value=0.0)
        with col2:
            price = st.number_input("Price per unit (optional)", min_value=0.0, step=1.0, value=0.0)
        with col3:
            notes = st.text_input("Notes", placeholder="Bill no / supplier / remarks")
        if st.button("Save Purchase"):
            if qty <= 0:
                st.error("Quantity must be > 0.")
            else:
                add_move("purchase", p["id"], qty, price_per_unit=(price or None), notes=notes or None)
                st.success("Purchase saved.")
        st.caption(f"Current stock: **{product_stock(p['id'])} {p['unit']}**")

# --- Sale (OUT) ---
with tabs[3]:
    st.subheader("Record Sale (Stock Out)")
    prods = list_products()
    custs = list_customers()
    if not prods:
        st.warning("Add a product first.")
    else:
        prod_map = {f'{p["name"]} ({p["size"] or ""} | {p["unit"]})': p for p in prods}
        choice = st.selectbox("Product*", list(prod_map.keys()), key="sale_product")
        p = prod_map[choice]
        stock_now = product_stock(p["id"])
        col1, col2, col3 = st.columns(3)
        with col1:
            qty = st.number_input(f"Quantity to sell ({p['unit']})*", min_value=0.0, step=1.0, value=0.0)
        with col2:
            price = st.number_input("Selling price per unit (optional)", min_value=0.0, step=1.0, value=0.0)
        with col3:
            customer_name = None
            customer_id = None
            if custs:
                cust_map = {c["name"]: c for c in custs}
                sel = st.selectbox("Customer (optional)", ["-- none --"] + list(cust_map.keys()))
                if sel != "-- none --":
                    customer_id = cust_map[sel]["id"]
                    customer_name = sel
            else:
                st.caption("Tip: add customers in the Customers tab.")
        notes = st.text_input("Notes", placeholder="Invoice no / payment mode / remarks")

        # ALLOW NEGATIVE STOCK: remove the check that blocked sales > stock_now
        if st.button("Save Sale"):
            if qty <= 0:
                st.error("Quantity must be > 0.")
            else:
                add_move("sale", p["id"], qty, price_per_unit=(price or None),
                         customer_id=customer_id, notes=notes or None)
                st.success(f"Sale saved. (Customer: {customer_name or 'N/A'})")

        # Live stock indicator
        if stock_now < 0:
            st.caption(f"<span class='negative'>Current stock: {stock_now} {p['unit']} (negative)</span>", unsafe_allow_html=True)
        else:
            st.caption(f"Current stock: **{stock_now} {p['unit']}**")

# --- Stock & Low Stock ---
with tabs[4]:
    st.subheader("Stock Levels")
    prods = list_products()
    if prods:
        df = pd.DataFrame(prods)
        df["current_stock"] = df["id"].apply(product_stock)
        # Add a readable warning column
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

        # Export
        if st.button("Export Stock to CSV"):
            out = df[["name","material","size","unit","current_stock"]].copy()
            out.to_csv("stock_export.csv", index=False)
            st.success("Saved as stock_export.csv (in the same folder).")
    else:
        st.info("No products yet.")

# --- Daily Report ---
with tabs[5]:
    st.subheader("Daily Report (Sales & Purchases)")
    day = st.date_input("Pick a date", value=date.today())
    rows = moves_on_day(day)
    if rows:
        rep = pd.DataFrame(rows)
        # readable columns
        rep["time"] = pd.to_datetime(rep["ts"]).dt.strftime("%H:%M")
        rep["qty_display"] = rep.apply(lambda r: f'{abs(r["qty"])} {r["unit"]}', axis=1)
        rep["value"] = rep.apply(lambda r: (abs(r["qty"]) * (r["price_per_unit"] or 0.0)), axis=1)
        rep["neg_stock_note"] = ""  # placeholder for optional future per-line note

        st.markdown("#### All Movements Today")
        show = rep[["time","kind","product_name","qty_display","customer_name","price_per_unit","value","notes"]]
        show = show.rename(columns={
            "kind":"Type","product_name":"Product","customer_name":"Customer",
            "price_per_unit":"Rate","value":"Amount","qty_display":"Qty"
        })
        st.dataframe(show, use_container_width=True)

        # Sales summary by customer
        sales = rep[rep["kind"]=="sale"].copy()
        if not sales.empty:
            st.markdown("#### Who bought today (Sales by Customer)")
            cust = sales.groupby("customer_name", dropna=False)["value"].sum().reset_index().rename(
                columns={"customer_name":"Customer","value":"Total Amount"}
            )
            cust["Customer"] = cust["Customer"].fillna("N/A")
            st.dataframe(cust, use_container_width=True)

        # Stock snapshot
        st.markdown("#### Stock Snapshot (End of Day)")
        prods = list_products()
        snap = []
        for p in prods:
            qty_left = product_stock(p["id"])
            snap.append({
                "Product": p["name"],
                "Size": p["size"],
                "Unit": p["unit"],
                "Stock Left": qty_left,
                "Status": "NEGATIVE ⚠️" if qty_left < 0 else ""
            })
        st.dataframe(pd.DataFrame(snap).sort_values("Product"), use_container_width=True)

        # Export
        if st.button("Export Today’s Report to CSV"):
            show.to_csv(f"report_{day.isoformat()}.csv", index=False)
            st.success(f"Saved as report_{day.isoformat()}.csv")
    else:
        st.info("No entries on this day yet.")

st.divider()
st.caption("Tip: Use ‘Purchase’ for stock coming in and ‘Sale’ for stock going out. Daily Report shows stock left and who bought.")
