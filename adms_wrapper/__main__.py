import pandas as pd
from .core.db_queries import get_attendences, get_device_logs, get_finger_log, get_migrations, get_users


# --- Data Extraction Functions ---
def fetch_all_data():
    """Fetch all required data from the database using helper functions."""
    attendences = get_attendences() or []
    device_logs = get_device_logs() or []
    finger_logs = get_finger_log() or []
    migration_logs = get_migrations() or []
    user_logs = get_users() or []
    return attendences, device_logs, finger_logs, migration_logs, user_logs


# --- Attendance Summary Processing ---
def get_device_for_time(group, time_col, sn_col, which):
    """
    Helper function to get the device (sn) for the first or last timestamp in a group.
    Args:
        group: DataFrame group for a single employee and day
        time_col: Name of the timestamp column
        sn_col: Name of the device column
        which: 'min' for first, 'max' for last
    Returns:
        Device (sn) value for the first or last timestamp
    """
    idx = group[time_col].idxmin() if which == "min" else group[time_col].idxmax()
    return group.loc[idx, sn_col] if idx in group.index else None


def process_attendance_summary(attendences):
    """
    Process the attendences data to create a summary DataFrame with:
    - employee_id
    - day
    - start_time (first entry)
    - end_time (last entry)
    - start_sn (device for first entry)
    - end_sn (device for last entry)
    - time_spent (duration between first and last entry, formatted as HH:MM:SS)
    """
    df_att = pd.DataFrame(attendences)
    required_cols = {"employee_id", "timestamp", "sn"}
    if not required_cols.issubset(df_att.columns):
        print("Could not find 'employee_id', 'timestamp', or 'sn' columns in attendences data.")
        return None

    # Convert timestamp to datetime for accurate calculations
    df_att["timestamp"] = pd.to_datetime(df_att["timestamp"])
    # Extract date (day) from timestamp
    df_att["day"] = df_att["timestamp"].dt.date

    # Group by employee_id and day to get start/end times and devices
    summary = (
        df_att.groupby(["employee_id", "day"])
        .apply(
            lambda g: pd.Series(
                {
                    "start_time": g["timestamp"].min(),
                    "end_time": g["timestamp"].max(),
                    "start_device_sn": get_device_for_time(g, "timestamp", "sn", "min"),
                    "end_device_sn": get_device_for_time(g, "timestamp", "sn", "max"),
                }
            )
        )
        .reset_index()
    )
    # Calculate time spent as a timedelta
    summary["time_spent"] = summary["end_time"] - summary["start_time"]
    # Format time spent as HH:MM:SS
    summary["time_spent"] = summary["time_spent"].apply(lambda x: str(x).split(".")[0])
    return summary


# --- Excel Export Function ---
def export_to_excel(attendences, device_logs, finger_logs, migration_logs, user_logs, attendance_summary):
    """
    Export all data and the attendance summary to an Excel file with separate sheets.
    """
    with pd.ExcelWriter("output.xlsx", engine="openpyxl") as writer:
        pd.DataFrame(attendences).to_excel(writer, sheet_name="Attendences", index=False)
        pd.DataFrame(device_logs).to_excel(writer, sheet_name="DeviceLogs", index=False)
        pd.DataFrame(finger_logs).to_excel(writer, sheet_name="FingerLogs", index=False)
        pd.DataFrame(migration_logs).to_excel(writer, sheet_name="Migrations", index=False)
        pd.DataFrame(user_logs).to_excel(writer, sheet_name="Users", index=False)
        if attendance_summary is not None:
            attendance_summary.to_excel(writer, sheet_name="AttendanceSummary", index=False)


# --- Main Execution ---
def main(start_date: str = None, end_date: str = None):
    """
    Main function to orchestrate data fetching, processing, and exporting.
    Optionally filters attendences by a date range.
    """
    attendences, device_logs, finger_logs, migration_logs, user_logs = fetch_all_data()
    # Filter attendences by date range if provided
    if start_date or end_date:
        df = pd.DataFrame(attendences)
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            if start_date:
                df = df[df["timestamp"] >= pd.to_datetime(start_date)]
            if end_date:
                df = df[df["timestamp"] <= pd.to_datetime(end_date)]
            attendences = df.to_dict(orient="records")
    attendance_summary = process_attendance_summary(attendences)
    export_to_excel(attendences, device_logs, finger_logs, migration_logs, user_logs, attendance_summary)
    return "Data exported to output.xlsx"
