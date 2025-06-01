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