"""
Database utility module for multi-database support.
Supports MySQL (local) and PostgreSQL (Supabase) based on DB_PROVIDER env variable.
"""

import os
import sys
from dotenv import load_dotenv

# Conditional imports based on what's needed
try:
    import mysql.connector
    from mysql.connector import Error as MySQLError
except ImportError:
    mysql = None
    MySQLError = None

try:
    import psycopg2
    from psycopg2 import Error as PostgresError
except ImportError:
    psycopg2 = None
    PostgresError = None


class DatabaseConnection:
    """
    Database connection wrapper that supports multiple database types.
    Automatically selects the correct driver based on DB_PROVIDER env variable.
    """

    def __init__(self, env_path=None):
        """Initialize database connection configuration."""
        if env_path is None:
            project_root = os.path.join(os.path.dirname(__file__), "..")
            env_path = os.path.join(project_root, ".env")

        if not os.path.exists(env_path):
            raise RuntimeError(f"Error: .env file not found at {env_path}")

        load_dotenv(env_path)

        self.provider = os.getenv("DB_PROVIDER", "local").lower()
        self.connection = None
        self.cursor = None

        if self.provider not in ["local", "supabase"]:
            raise ValueError(f"Invalid DB_PROVIDER: {self.provider}. Must be 'local' or 'supabase'")

    def get_connection_params(self):
        """Get connection parameters based on provider."""
        if self.provider == "local":
            # MySQL local database - use generic DB_* variables
            return {
                "host": os.getenv("DB_HOST", "localhost"),
                "port": int(os.getenv("DB_PORT", 3306)),
                "database": os.getenv("DB_NAME"),
                "user": os.getenv("DB_USER"),
                "password": os.getenv("DB_PASSWORD"),
            }
        elif self.provider == "supabase":
            # PostgreSQL Supabase database - use SUPABASE_DB_* variables
            return {
                "host": os.getenv("SUPABASE_DB_HOST"),
                "port": int(os.getenv("SUPABASE_DB_PORT", 5432)),
                "database": os.getenv("SUPABASE_DB_NAME", "postgres"),
                "user": os.getenv("SUPABASE_DB_USER"),
                "password": os.getenv("SUPABASE_DB_PASSWORD"),
            }

    def connect(self):
        """Establish database connection."""
        params = self.get_connection_params()

        # Validate required parameters
        missing = [k for k, v in params.items() if v in (None, "")]
        if missing:
            raise RuntimeError(f"Missing required env variables: {', '.join(missing)}")

        try:
            if self.provider == "local":
                if mysql is None:
                    raise RuntimeError("mysql-connector-python is not installed. Run: pip install mysql-connector-python")
                self.connection = mysql.connector.connect(**params)
            elif self.provider == "supabase":
                if psycopg2 is None:
                    raise RuntimeError("psycopg2 is not installed. Run: pip install psycopg2-binary")
                self.connection = psycopg2.connect(**params)

            return self.connection

        except Exception as e:
            raise RuntimeError(f"Database connection error ({self.provider}): {e}")

    def get_cursor(self):
        """Get a database cursor."""
        if self.connection is None:
            self.connect()
        return self.connection.cursor()

    def commit(self):
        """Commit the current transaction."""
        if self.connection:
            self.connection.commit()

    def rollback(self):
        """Rollback the current transaction."""
        if self.connection:
            self.connection.rollback()

    def close(self):
        """Close cursor and connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if exc_type is not None:
            self.rollback()
        self.close()


def get_database_connection(env_path=None):
    """
    Factory function to get a database connection based on DB_PROVIDER.
    Returns a connection object compatible with both MySQL and PostgreSQL.

    Usage:
        conn = get_database_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM dams")
        results = cursor.fetchall()
        cursor.close()
        conn.close()

    Or with context manager:
        with DatabaseConnection() as db:
            cursor = db.get_cursor()
            cursor.execute("SELECT * FROM dams")
            results = cursor.fetchall()
            cursor.close()
            db.commit()
    """
    db = DatabaseConnection(env_path)
    return db.connect()


def test_connection():
    """Test database connection and print connection info."""
    try:
        with DatabaseConnection() as db:
            cursor = db.get_cursor()

            if db.provider == "local":
                cursor.execute("SELECT VERSION()")
                version = cursor.fetchone()[0]
                print(f"✓ Connected to MySQL")
                print(f"  Version: {version}")
                cursor.execute("SELECT DATABASE()")
                database = cursor.fetchone()[0]
                print(f"  Database: {database}")

            elif db.provider == "supabase":
                cursor.execute("SELECT version()")
                version = cursor.fetchone()[0]
                print(f"✓ Connected to PostgreSQL (Supabase)")
                print(f"  Version: {version}")
                cursor.execute("SELECT current_database()")
                database = cursor.fetchone()[0]
                print(f"  Database: {database}")

            cursor.close()
            print(f"  Provider: {db.provider}")
            return True

    except Exception as e:
        print(f"✗ Connection failed: {e}")
        return False


if __name__ == "__main__":
    """Test the connection utility."""
    print("=" * 60)
    print("Database Connection Test")
    print("=" * 60)

    success = test_connection()

    print("=" * 60)
    if success:
        print("✓ Connection test PASSED")
    else:
        print("✗ Connection test FAILED")
        sys.exit(1)
