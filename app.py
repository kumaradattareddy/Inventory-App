import sqlite3
from contextlib import closing
from datetime import datetime
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

def run_query(query, params=(), fetch=False):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(query, params)
        if fetch:
            rows = cur.fetchall()
            return [dict(r) for r in rows]
        conn.commit()

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

    if not user_exists(DEFAULT_USERNAME):
        create_user(DEFAULT_USERNAME, DEFAULT_PASSWORD)

# ---------- APP ----------
def main():
    st.title("📦 Inventory Management")

    # --- LOGIN ---
    if "user" not in st.session_state:
        st.sidebar.header("🔐 Login")
        with st.sidebar.form("login_form"):
            username = st.text_input("Username", key="login_user")
            password = st.text_input("Password", type="password", key="login_pass")
            submit = st.form_submit_button("Login")
        if submit:
            user = verify_login(username, password)
            if user:
                st.session_state["user"] = user
                st.success("✅ Logged in successfully!")
                st.rerun()
            else:
                st.error("❌ Invalid login")
        return

    st.sidebar.success(f"Logged in as {st.session_state['user']['username']}")
    page = st.sidebar.radio("Go to", ["Products", "Customers", "Stock Moves", "Reports"])

    # --- PRODUCTS ---
    if page == "Products":
        st.header("📦 Manage Products")

        with st.form("product_form", clear_on_submit=True):
            name = st.text_input("Product Name", key="prod_name")
            material = st.text_input("Material", key="prod_material")
            size = st.text_input("Size", key="prod_size")
            unit = st.text_input("Unit", key="prod_unit")
            opening_stock = st.number_input("Opening Stock", min_value=0.0, step=0.1, key="prod_opening")
            submitted = st.form_submit_button("➕ Add Product")

        if submitted:
            run_query("INSERT INTO products(name, material, size, unit, opening_stock) VALUES(?,?,?,?,?)",
                      (name, material, size, unit, opening_stock))
            st.success("✅ Product added!")

        st.subheader("Existing Products")
        products = run_query("SELECT * FROM products", fetch=True)
        st.dataframe(pd.DataFrame(products))

    # --- CUSTOMERS ---
    elif page == "Customers":
        st.header("👥 Manage Customers")

        with st.form("customer_form", clear_on_submit=True):
            name = st.text_input("Customer Name", key="cust_name")
            phone = st.text_input("Phone", key="cust_phone")
            address = st.text_area("Address", key="cust_address")
            submitted = st.form_submit_button("➕ Add Customer")

        if submitted:
            run_query("INSERT INTO customers(name, phone, address) VALUES(?,?,?)", (name, phone, address))
            st.success("✅ Customer added!")

        st.subheader("Existing Customers")
        customers = run_query("SELECT * FROM customers", fetch=True)
        st.dataframe(pd.DataFrame(customers))

    # --- STOCK MOVES ---
    elif page == "Stock Moves":
        st.header("📦 Add Stock Movement")

        with st.form("stock_move_form", clear_on_submit=True):
            kind = st.selectbox("Kind", ["IN", "OUT"], key="move_kind")
            product = st.selectbox(
                "Product",
                [f"{p['id']} - {p['name']}" for p in run_query("SELECT * FROM products", fetch=True)],
                key="move_product"
            )
            qty = st.number_input("Quantity", min_value=0.0, step=0.1, key="move_qty")
            unit_price = st.number_input("Price per unit", min_value=0.0, step=0.1, key="move_price")
            customer = st.selectbox(
                "Customer (optional)",
                ["None"] + [f"{c['id']} - {c['name']}" for c in run_query("SELECT * FROM customers", fetch=True)],
                key="move_customer"
            )
            notes = st.text_area("Notes", key="move_notes")

            submitted = st.form_submit_button("💾 Save Movement")

        if submitted:
            product_id = int(product.split(" - ")[0])
            cust_id = None if customer == "None" else int(customer.split(" - ")[0])
            run_query("""
                INSERT INTO stock_moves(ts, kind, product_id, qty, price_per_unit, customer_id, notes)
                VALUES(?,?,?,?,?,?,?)
            """, (datetime.now().isoformat(), kind, product_id, qty, unit_price, cust_id, notes))
            st.success("✅ Stock movement saved!")

        st.subheader("All Stock Movements")
        moves = run_query("SELECT * FROM stock_moves ORDER BY ts DESC", fetch=True)
        st.dataframe(pd.DataFrame(moves))

    # --- REPORTS ---
    elif page == "Reports":
        st.header("📊 Reports")

        stock = run_query("""
            SELECT p.name, p.unit, p.opening_stock +
                IFNULL((SELECT SUM(qty) FROM stock_moves WHERE product_id=p.id AND kind='IN'),0) -
                IFNULL((SELECT SUM(qty) FROM stock_moves WHERE product_id=p.id AND kind='OUT'),0)
                AS current_stock
            FROM products p
        """, fetch=True)
        st.subheader("📦 Current Stock Levels")
        st.dataframe(pd.DataFrame(stock))


if __name__ == "__main__":
    init_db()
    main()
