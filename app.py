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

def run_query(query: str, params=(), fetch=False):
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

    # Ensure default user exists
    if not user_exists(DEFAULT_USERNAME):
        create_user(DEFAULT_USERNAME, DEFAULT_PASSWORD)


# ---------- STREAMLIT APP ----------
st.set_page_config(page_title="Inventory App", layout="wide")
init_db()

# Session state for login
if "user" not in st.session_state:
    st.session_state.user = None

# --- LOGIN SCREEN ---
if not st.session_state.user:
    st.title("🔐 Login")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    if st.button("Login"):
        user = verify_login(username, password)
        if user:
            st.session_state.user = user
            st.success(f"Welcome, {user['username']} 👋")
            st.rerun()
        else:
            st.error("Invalid login")

else:
    # --- MAIN APP ---
    st.sidebar.success(f"Logged in as {st.session_state.user['username']}")
    if st.sidebar.button("Logout"):
        st.session_state.user = None
        st.rerun()

    st.title("📦 Inventory Management")

    tab1, tab2, tab3 = st.tabs(["Products", "Customers", "Stock Moves"])

    with tab1:
        st.subheader("Products")
        products = run_query("SELECT * FROM products", fetch=True)
        st.dataframe(pd.DataFrame(products) if products else pd.DataFrame())
        with st.form("add_product"):
            name = st.text_input("Product Name")
            material = st.text_input("Material")
            size = st.text_input("Size")
            unit = st.selectbox("Unit", ["pcs", "box", "sqft", "kg", "bag"])
            opening = st.number_input("Opening Stock", min_value=0.0, step=1.0)
            if st.form_submit_button("Add Product"):
                run_query("INSERT INTO products(name, material, size, unit, opening_stock) VALUES (?,?,?,?,?)",
                          (name, material, size, unit, opening))
                st.success("Product added!")
                st.rerun()

    with tab2:
        st.subheader("Customers")
        customers = run_query("SELECT * FROM customers", fetch=True)
        st.dataframe(pd.DataFrame(customers) if customers else pd.DataFrame())
        with st.form("add_customer"):
            cname = st.text_input("Customer Name")
            phone = st.text_input("Phone")
            addr = st.text_area("Address")
            if st.form_submit_button("Add Customer"):
                run_query("INSERT INTO customers(name, phone, address) VALUES (?,?,?)",
                          (cname, phone, addr))
                st.success("Customer added!")
                st.rerun()

    with tab3:
        st.subheader("Stock Moves")
        moves = run_query("SELECT * FROM stock_moves", fetch=True)
        st.dataframe(pd.DataFrame(moves) if moves else pd.DataFrame())
        with st.form("add_move"):
            kind = st.selectbox("Move Type", ["IN", "OUT"])
            prod_id = st.number_input("Product ID", min_value=1, step=1)
            qty = st.number_input("Quantity", min_value=0.0, step=1.0)
            price = st.number_input("Price per unit", min_value=0.0, step=1.0)
            cust_id = st.number_input("Customer ID (optional)", min_value=0, step=1)
            notes = st.text_area("Notes")
            if st.form_submit_button("Add Move"):
                run_query("INSERT INTO stock_moves(ts, kind, product_id, qty, price_per_unit, customer_id, notes) VALUES (?,?,?,?,?,?,?)",
                          (datetime.now().isoformat(), kind, prod_id, qty, price, cust_id if cust_id > 0 else None, notes))
                st.success("Move added!")
                st.rerun()
