
from .db_connector import query_db

def get_attendences() -> list:
    """Gets the attendence related data"""
    return query_db("select * from attendances a ")

def get_device_logs() -> list:
    """Gets the device log history, as in when a device was registered, etc"""
    return query_db("select * from device_log a ")

def get_finger_log() -> list:
    """Gets a log of all the available fingers that were logged into the system and when they were created"""
    return query_db("select * from device_log a ")

def get_migrations() -> list:
    """Get a list of all the migrations made to the SQL table"""
    return query_db("select * from migrations a ")

def get_users() -> list:
    """Get a list of all the registered users in the system"""
    return query_db("select * from users a ")

# --- Device Serial Number to Branch Name Mapping ---

def create_branch_mapping_table():
    """Create the branch mapping table if it does not exist, with unique serial_number."""
    query = '''
    CREATE TABLE IF NOT EXISTS branch_mapping (
        id INT AUTO_INCREMENT PRIMARY KEY,
        serial_number VARCHAR(255) NOT NULL UNIQUE,
        branch_name VARCHAR(255) NOT NULL
    )
    '''
    query_db(query)

def get_device_branch_mappings() -> list:
    """Get all device serial number to branch name mappings."""
    create_branch_mapping_table()
    return query_db("SELECT serial_number, branch_name FROM branch_mapping")


def add_device_branch_mapping(serial_number: str, branch_name: str):
    """Add or update a device serial number to branch name mapping. Creates the table if it does not exist."""
    create_branch_mapping_table()
    query = '''
    INSERT INTO branch_mapping (serial_number, branch_name)
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE branch_name = VALUES(branch_name)
    '''
    return query_db(query, (serial_number, branch_name))
    
def delete_device_branch_mapping(serial_number: str):
    """Delete a device serial number to branch name mapping."""
    query = "DELETE FROM branch_mapping WHERE serial_number = %s"
    return query_db(query, (serial_number,))