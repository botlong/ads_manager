import sqlite3
import hashlib
import os
from datetime import datetime, timedelta
import secrets

# Path to DB
DB_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'ads_data.sqlite')

# --- Database Initialization ---
def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def init_users_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL, -- e.g., admin, manager, viewer
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS active_tokens (
            token TEXT PRIMARY KEY,
            username TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            FOREIGN KEY (username) REFERENCES users(username)
        )
    """)
    conn.commit()
    conn.close()
    print("User and token tables initialized.")

# --- Password Hashing ---
def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    hashed_password = hashlib.sha256((password + salt).encode('utf-8')).hexdigest()
    return f"{salt}${hashed_password}"

def verify_password(stored_password_hash: str, provided_password: str) -> bool:
    salt, hashed_password = stored_password_hash.split('$')
    return hashed_password == hashlib.sha256((provided_password + salt).encode('utf-8')).hexdigest()

# --- User Management ---
def create_user(username: str, password: str, role: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        password_hash = hash_password(password)
        cursor.execute("INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)",
                       (username, password_hash, role))
        conn.commit()
        print(f"User '{username}' ({role}) created successfully.")
        return True
    except sqlite3.IntegrityError:
        print(f"User '{username}' already exists.")
        return False
    finally:
        conn.close()

def get_user(username: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE username = ?", (username,))
    user = cursor.fetchone()
    conn.close()
    return user

def setup_default_admin_users():
    init_users_db() # Ensure tables exist
    users_to_create = [
        ("admin", "adminpass", "admin"),
        ("manager", "managerpass", "manager"),
        ("viewer", "viewerpass", "viewer"),
    ]
    for username, password, role in users_to_create:
        create_user(username, password, role)

# --- Token Management ---
def generate_token(username: str) -> str:
    token = secrets.token_urlsafe(32)
    expires_at = (datetime.now() + timedelta(hours=24)).isoformat() # Token valid for 24 hours

    conn = get_db_connection()
    cursor = conn.cursor()
    # Clean up old tokens for this user (optional, but good for security)
    cursor.execute("DELETE FROM active_tokens WHERE username = ?", (username,))
    cursor.execute("INSERT INTO active_tokens (token, username, expires_at) VALUES (?, ?, ?)",
                   (token, username, expires_at))
    conn.commit()
    conn.close()
    return token

def verify_token(token: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT username, expires_at FROM active_tokens WHERE token = ?", (token,))
    result = cursor.fetchone()
    conn.close()

    if result:
        username, expires_at_str = result
        expires_at = datetime.fromisoformat(expires_at_str)
        if expires_at > datetime.now():
            return username # Token is valid
    
    return None # Token is invalid or expired

def invalidate_token(token: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM active_tokens WHERE token = ?", (token,))
    conn.commit()
    conn.close()
    print(f"Token invalidated.")

# Run setup when auth.py is imported
setup_default_admin_users()
