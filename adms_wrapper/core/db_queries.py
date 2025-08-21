# --- User Shift Mapping ---
def create_shift_template_table():
    """Create the shift_template table if it does not exist."""
    query = """
    CREATE TABLE IF NOT EXISTS shift_template (
        id INT AUTO_INCREMENT PRIMARY KEY,
        shift_name VARCHAR(255) NOT NULL UNIQUE,
        shift_start TIME NOT NULL,
        shift_end TIME NOT NULL,
        description VARCHAR(500)
    )
    """
    query_db(query)


def get_shift_templates() -> list:
    """Get all shift templates."""
    create_shift_template_table()
    return query_db("SELECT shift_name, shift_start, shift_end, description FROM shift_template ORDER BY shift_name")


def add_shift_template(shift_name: str, shift_start: str, shift_end: str, description: str = ""):
    """Add or update a shift template."""
    create_shift_template_table()
    query = """
    INSERT INTO shift_template (shift_name, shift_start, shift_end, description)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE shift_start=VALUES(shift_start), shift_end=VALUES(shift_end), description=VALUES(description)
    """
    return query_db(query, (shift_name, shift_start, shift_end, description))


def delete_shift_template(shift_name: str):
    """Delete a shift template."""
    query = "DELETE FROM shift_template WHERE shift_name = %s"
    return query_db(query, (shift_name,))


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


def assign_shift_template_to_user(user_id: str, shift_name: str):
    """Assign a shift template to a user by copying the template times."""
    create_shift_template_table()
    create_user_shift_mapping_table()
    
    # Get the shift template
    template_query = "SELECT shift_start, shift_end FROM shift_template WHERE shift_name = %s"
    template_result = query_db(template_query, (shift_name,))
    
    if not template_result:
        raise ValueError(f"Shift template '{shift_name}' not found")
    
    shift_start = template_result[0]["shift_start"]
    shift_end = template_result[0]["shift_end"]
    
    # Assign to user
    return add_user_shift_mapping(user_id, shift_name, str(shift_start), str(shift_end))


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


# --- Employee ID to Name Mapping ---


def create_employee_name_mapping_table() -> None:
    """Create the employee name mapping table if it does not exist, with unique employee_id."""
    query = """
    CREATE TABLE IF NOT EXISTS employee_name_mapping (
        id INT AUTO_INCREMENT PRIMARY KEY,
        employee_id VARCHAR(255) NOT NULL UNIQUE,
        employee_name VARCHAR(255) NOT NULL
    )
    """
    query_db(query)


def get_employee_name_mappings() -> list:
    """Get all employee ID to name mappings."""
    create_employee_name_mapping_table()
    return query_db("SELECT employee_id, employee_name FROM employee_name_mapping")


def add_employee_name_mapping(employee_id: str, employee_name: str) -> list:
    """Add or update an employee ID to name mapping. Creates the table if it does not exist."""
    create_employee_name_mapping_table()
    query = """
    INSERT INTO employee_name_mapping (employee_id, employee_name)
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE employee_name = VALUES(employee_name)
    """
    return query_db(query, (employee_id, employee_name))


def delete_employee_name_mapping(employee_id: str) -> list:
    """Delete an employee ID to name mapping."""
    query = "DELETE FROM employee_name_mapping WHERE employee_id = %s"
    return query_db(query, (employee_id,))


# --- Comprehensive Employee Management ---
def get_comprehensive_employee_data(employee_id: str = None):
    """Get comprehensive employee data including all mappings."""
    if employee_id:
        # Get data for specific employee
        employee_data = {
            "employee_id": employee_id,
            "employee_name": "",
            "designation": "",
            "branch_name": "",
            "shift_name": "",
            "shift_start": "",
            "shift_end": ""
        }
        
        # Get employee name
        name_mappings = get_employee_name_mappings() or []
        for mapping in name_mappings:
            if mapping["employee_id"] == employee_id:
                employee_data["employee_name"] = mapping["employee_name"]
                break
        
        # Get designation
        designation_mappings = get_employee_designation_mappings() or []
        for mapping in designation_mappings:
            if mapping["employee_id"] == employee_id:
                employee_data["designation"] = mapping["designation"]
                break
        
        # Get branch
        branch_mappings = get_employee_branch_mappings() or []
        for mapping in branch_mappings:
            if mapping["employee_id"] == employee_id:
                employee_data["branch_name"] = mapping["branch_name"]
                break
        
        # Get shift
        shift_mappings = get_user_shift_mappings() or []
        for mapping in shift_mappings:
            if mapping["user_id"] == employee_id:
                employee_data["shift_name"] = mapping["shift_name"]
                employee_data["shift_start"] = str(mapping["shift_start"])
                employee_data["shift_end"] = str(mapping["shift_end"])
                break
        
        return employee_data
    else:
        # Get all employees with their comprehensive data
        all_employee_ids = set()
        
        # Collect all unique employee IDs from all mapping tables
        for mapping in get_employee_name_mappings() or []:
            all_employee_ids.add(mapping["employee_id"])
        for mapping in get_employee_designation_mappings() or []:
            all_employee_ids.add(mapping["employee_id"])
        for mapping in get_employee_branch_mappings() or []:
            all_employee_ids.add(mapping["employee_id"])
        for mapping in get_user_shift_mappings() or []:
            all_employee_ids.add(mapping["user_id"])
        
        # Get comprehensive data for each employee
        all_employees = []
        for emp_id in sorted(all_employee_ids):
            all_employees.append(get_comprehensive_employee_data(emp_id))
        
        return all_employees


def add_comprehensive_employee(employee_id: str, employee_name: str = "", designation: str = "", 
                             branch_name: str = "", shift_name: str = ""):
    """Add or update comprehensive employee data."""
    results = []
    
    # Add employee name if provided
    if employee_name:
        results.append(add_employee_name_mapping(employee_id, employee_name))
    
    # Add designation if provided
    if designation:
        results.append(add_employee_designation_mapping(employee_id, designation))
    
    # Add branch if provided
    if branch_name:
        results.append(add_employee_branch_mapping(employee_id, branch_name))
    
    # Add shift if provided
    if shift_name:
        try:
            results.append(assign_shift_template_to_user(employee_id, shift_name))
        except ValueError:
            # If shift template doesn't exist, skip
            pass
    
    return results


def delete_comprehensive_employee(employee_id: str):
    """Delete all mappings for an employee."""
    results = []
    
    # Delete from all mapping tables
    try:
        results.append(delete_employee_name_mapping(employee_id))
    except:
        pass
    
    try:
        results.append(delete_employee_designation_mapping(employee_id))
    except:
        pass
    
    try:
        results.append(delete_employee_branch_mapping(employee_id))
    except:
        pass
    
    try:
        results.append(delete_user_shift_mapping(employee_id))
    except:
        pass
    
    return results
