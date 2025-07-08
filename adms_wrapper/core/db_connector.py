import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER', 'root'),  
    'password': os.getenv('DB_PASSWORD', 'test123'),  
    'database': os.getenv('DB_DATABASE', 'laravel'),
    'port': int(os.getenv('DB_PORT', '3306')),
}

def get_connection():
    """Create and return a new MySQL connection to the larvel database."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

def query_db(query, params=None):
    """Execute a query and return the results as a list of dicts."""
    conn = get_connection()
    if not conn:
        return None
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query, params or ())
        results = cursor.fetchall()
        return results
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
    temp_config.pop('database', None)  

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

