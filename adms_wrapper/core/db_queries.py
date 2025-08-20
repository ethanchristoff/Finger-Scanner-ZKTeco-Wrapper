# --- User Shift Mapping ---
def create_user_shift_mapping_table():
    """Create the user_shift_mapping table if it does not exist."""
    query = """
    CREATE TABLE IF NOT EXISTS user_shift_mapping (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_id VARCHAR(255) NOT NULL,
        shift_name VARCHAR(255) NOT NULL,
        shift_start TIME NOT NULL,
        shift_end TIME NOT NULL
    )
    """
    query_db(query)


def get_user_shift_mappings() -> list:
    """Get all user shift mappings."""
    create_user_shift_mapping_table()
    return query_db("SELECT user_id, shift_name, shift_start, shift_end FROM user_shift_mapping")


def add_user_shift_mapping(user_id: str, shift_name: str, shift_start: str, shift_end: str):
    """Add or update a user shift mapping."""
    create_user_shift_mapping_table()
    query = """
    INSERT INTO user_shift_mapping (user_id, shift_name, shift_start, shift_end)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE shift_name=VALUES(shift_name), shift_start=VALUES(shift_start), shift_end=VALUES(shift_end)
    """
    return query_db(query, (user_id, shift_name, shift_start, shift_end))


def delete_user_shift_mapping(user_id: str):
    """Delete a user shift mapping."""
    query = "DELETE FROM user_shift_mapping WHERE user_id = %s"
    return query_db(query, (user_id,))


# --- Employee to Branch Mapping ---
def create_employee_branch_mapping_table():
    """Create the employee_branch_mapping table if it does not exist."""
    query = """
    CREATE TABLE IF NOT EXISTS employee_branch_mapping (
        id INT AUTO_INCREMENT PRIMARY KEY,
        employee_id VARCHAR(255) NOT NULL UNIQUE,
        branch_name VARCHAR(255) NOT NULL
    )
    """
    query_db(query)


def get_employee_branch_mappings() -> list:
    """Get all employee branch mappings."""
    create_employee_branch_mapping_table()
    return query_db("SELECT employee_id, branch_name FROM employee_branch_mapping")


def add_employee_branch_mapping(employee_id: str, branch_name: str):
    """Add or update an employee branch mapping."""
    create_employee_branch_mapping_table()
    query = """
    INSERT INTO employee_branch_mapping (employee_id, branch_name)
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE branch_name=VALUES(branch_name)
    """
    return query_db(query, (employee_id, branch_name))


def delete_employee_branch_mapping(employee_id: str):
    """Delete an employee branch mapping."""
    query = "DELETE FROM employee_branch_mapping WHERE employee_id = %s"
    return query_db(query, (employee_id,))


from .db_connector import query_db


def get_attendences() -> list:
    """Gets the attendence related data"""
    return query_db("select * from attendances a ")


def get_device_logs() -> list:
    """Gets the device log history, as in when a device was registered, etc"""
    return query_db("select * from device_log a ")


def get_finger_log() -> list:
    """Gets a log of all the available fingers that were logged into the system and when they were created"""
    return query_db("select * from finger_log a ")


def get_migrations() -> list:
    """Get a list of all the migrations made to the SQL table"""
    return query_db("select * from migrations a ")


def get_users() -> list:
    """Get a list of all the registered users in the system"""
    return query_db("select * from users a ")


# --- Device Serial Number to Branch Name Mapping ---


def create_branch_mapping_table():
    """Create the branch mapping table if it does not exist, with unique serial_number."""
    query = """
    CREATE TABLE IF NOT EXISTS branch_mapping (
        id INT AUTO_INCREMENT PRIMARY KEY,
        serial_number VARCHAR(255) NOT NULL UNIQUE,
        branch_name VARCHAR(255) NOT NULL
    )
    """
    query_db(query)


def get_device_branch_mappings() -> list:
    """Get all device serial number to branch name mappings."""
    create_branch_mapping_table()
    return query_db("SELECT serial_number, branch_name FROM branch_mapping")


def add_device_branch_mapping(serial_number: str, branch_name: str):
    """Add or update a device serial number to branch name mapping. Creates the table if it does not exist."""
    create_branch_mapping_table()
    query = """
    INSERT INTO branch_mapping (serial_number, branch_name)
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE branch_name = VALUES(branch_name)
    """
    return query_db(query, (serial_number, branch_name))


def delete_device_branch_mapping(serial_number: str):
    """Delete a device serial number to branch name mapping."""
    query = "DELETE FROM branch_mapping WHERE serial_number = %s"
    return query_db(query, (serial_number,))


# --- Employee ID to Designation Mapping ---


def create_employee_designation_mapping_table() -> None:
    """Create the employee designation mapping table if it does not exist, with unique employee_id."""
    query = """
    CREATE TABLE IF NOT EXISTS employee_designation_mapping (
        id INT AUTO_INCREMENT PRIMARY KEY,
        employee_id VARCHAR(255) NOT NULL UNIQUE,
        designation VARCHAR(255) NOT NULL
    )
    """
    query_db(query)


def get_employee_designation_mappings() -> list:
    """Get all employee ID to designation mappings."""
    create_employee_designation_mapping_table()
    return query_db("SELECT employee_id, designation FROM employee_designation_mapping")


def add_employee_designation_mapping(employee_id: str, designation: str) -> list:
    """Add or update an employee ID to designation mapping. Creates the table if it does not exist."""
    create_employee_designation_mapping_table()
    query = """
    INSERT INTO employee_designation_mapping (employee_id, designation)
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE designation = VALUES(designation)
    """
    return query_db(query, (employee_id, designation))


def delete_employee_designation_mapping(employee_id: str) -> list:
    """Delete an employee ID to designation mapping."""
    query = "DELETE FROM employee_designation_mapping WHERE employee_id = %s"
    return query_db(query, (employee_id,))
