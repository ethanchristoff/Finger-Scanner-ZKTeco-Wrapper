import os
import time
from dotenv import load_dotenv

import mysql.connector
from mysql.connector import Error
from mysql.connector import pooling

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", "test123"),
    "database": os.getenv("DB_DATABASE", "laravel"),
    "port": int(os.getenv("DB_PORT", "3306")),
}

_POOL_NAME = os.getenv("DB_POOL_NAME", "adms_pool")
_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "5"))
_CONNECT_TIMEOUT = int(os.getenv("DB_CONNECT_TIMEOUT", "5"))
_MAX_RETRIES = int(os.getenv("DB_CONNECT_RETRIES", "3"))
_RETRY_BACKOFF_BASE = float(os.getenv("DB_CONNECT_BACKOFF_BASE", "0.5"))

_POOL: pooling.MySQLConnectionPool | None = None


def _init_pool() -> None:
    """Initialize a MySQL connection pool if not already created."""
    global _POOL
    if _POOL is not None:
        return
    try:
        _POOL = pooling.MySQLConnectionPool(
            pool_name=_POOL_NAME,
            pool_size=_POOL_SIZE,
            pool_reset_session=True,
            connection_timeout=_CONNECT_TIMEOUT,
            **DB_CONFIG,
        )
    except Exception as e:
        _POOL = None


def get_connection():
    """Return a MySQL connection. Prefer pooled connections; fall back to direct connect with retries/backoff."""
    global _POOL
    if _POOL is None:
        _init_pool()

    last_exc = None
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            if _POOL is not None:
                conn = _POOL.get_connection()
            else:
                conn = mysql.connector.connect(connection_timeout=_CONNECT_TIMEOUT, **DB_CONFIG)

            # Quick sanity check
            if conn and conn.is_connected():
                return conn
            # If not connected, close and raise to trigger retry
            try:
                conn.close()
            except Exception:
                pass
            raise Error("Failed to obtain connected DB connection")
        except Exception as e:
            last_exc = e
            wait = _RETRY_BACKOFF_BASE * (2 ** (attempt - 1))
            print(f"DB connection attempt {attempt} failed: {e}; retrying in {wait:.2f}s")
            time.sleep(wait)

    # All retries failed
    print(f"Error connecting to MySQL after {_MAX_RETRIES} attempts: {last_exc}")
    return None


def query_db(query, params=None):
    """Execute a query and return the results as a list of dicts."""
    conn = get_connection()
    if not conn:
        return None
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query, params or ())

        # If the query returned rows (SELECT), fetch and return them
        if cursor.with_rows:
            results = cursor.fetchall()
            return results

        conn.commit()
        return cursor.rowcount
    except Error as e:
        print(f"Query failed: {e}")
        return None
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()


def list_databases():
    """List all available databases on the MySQL server."""
    temp_config = DB_CONFIG.copy()
    temp_config.pop("database", None)

    conn = None
    try:
        conn = mysql.connector.connect(**temp_config)
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("SHOW DATABASES")
            databases = [db[0] for db in cursor.fetchall()]
            return databases
    except Error as e:
        print(f"Error listing databases: {e}")
        return None
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
