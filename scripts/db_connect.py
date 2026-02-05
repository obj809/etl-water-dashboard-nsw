# scripts/db_connect.py

import os
import sys
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")


def load_env():
    dotenv_path = os.path.join(PROJECT_ROOT, ".env")
    if not os.path.exists(dotenv_path):
        print(f"Error: .env file not found at {dotenv_path}")
        sys.exit(1)
    load_dotenv(dotenv_path)


def get_config():
    cfg = {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", 3306)),
        "database": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
    }
    missing = [k for k, v in cfg.items() if v in (None, "")]
    if missing:
        print(f"Error: Missing env vars: {', '.join(missing)}")
        sys.exit(1)
    return cfg


def test_connection(cfg):
    try:
        conn = mysql.connector.connect(**cfg)
        if conn.is_connected():
            print(f"✓ Connected to MySQL {conn.get_server_info()}")
            cur = conn.cursor()
            cur.execute("SELECT DATABASE();")
            print(f"Using database: {cur.fetchone()[0]}")
            cur.close()
    except Error as e:
        print(f"✗ Connection error: {e}")
        sys.exit(1)
    finally:
        if "conn" in locals() and conn.is_connected():
            conn.close()
            print("MySQL connection closed.")


def main():
    load_env()
    cfg = get_config()
    test_connection(cfg)


if __name__ == "__main__":
    main()
