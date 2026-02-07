# scripts/db_connect.py

"""
Database connection testing utility.
Supports both MySQL (local) and PostgreSQL (Supabase) based on DB_PROVIDER.
"""

import os
import sys

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(__file__))

from db_utils import test_connection


def main():
    """Test database connection based on DB_PROVIDER."""
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


if __name__ == "__main__":
    main()
