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
    - work_status (worked/absent)

    This function now includes all working days (excluding Sundays) for each employee,
    showing both days they worked and days they were absent.
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

    # Group by employee_id and day to get start/end times and devices for actual attendance
    worked_summary = (
        df_att.groupby(["employee_id", "day"])
        .apply(
            lambda g: pd.Series(
                {
                    "start_time": g["timestamp"].min(),
                    "end_time": g["timestamp"].max(),
                    "start_device_sn": get_device_for_time(g, "timestamp", "sn", "min"),
                    "end_device_sn": get_device_for_time(g, "timestamp", "sn", "max"),
                    "work_status": "worked",
                    "num_entries": len(g),  # Count number of entries for the day
                }
            )
        )
        .reset_index()
    )

    # Calculate time spent for worked days with improved logic for missing sign-outs
    def calculate_time_spent(row):
        start_time = row["start_time"]
        end_time = row["end_time"]
        num_entries = row["num_entries"]
        day = row["day"]

        # If only one entry (sign-in but no sign-out), calculate time until end of day
        if num_entries == 1:
            # Set end time to end of the same day (23:59:59)
            end_of_day = pd.Timestamp.combine(day, pd.Timestamp("23:59:59").time())
            time_diff = end_of_day - start_time
        else:
            # Normal case: calculate difference between start and end times
            time_diff = end_time - start_time

        return str(time_diff).split(".")[0]

    worked_summary["time_spent"] = worked_summary.apply(calculate_time_spent, axis=1)

    # Update end_time for single-entry days to reflect end of day
    def update_end_time(row):
        if row["num_entries"] == 1:
            # Set end time to end of the same day for display purposes
            return pd.Timestamp.combine(row["day"], pd.Timestamp("23:59:59").time())
        return row["end_time"]

    worked_summary["end_time"] = worked_summary.apply(update_end_time, axis=1)

    # Drop the helper column
    worked_summary = worked_summary.drop(columns=["num_entries"])

    # If no attendance data, return empty DataFrame
    if worked_summary.empty:
        return pd.DataFrame(columns=["employee_id", "day", "start_time", "end_time", "start_device_sn", "end_device_sn", "time_spent", "work_status"])

    # Get all unique employees
    unique_employees = worked_summary["employee_id"].unique()

    # Get date range from the data
    min_date = worked_summary["day"].min()
    max_date = worked_summary["day"].max()

    # Generate all working days (excluding Sundays) in the date range
    date_range = pd.date_range(start=min_date, end=max_date, freq="D")
    working_days = [d.date() for d in date_range if d.weekday() != 6]  # 6 = Sunday

    # Create a complete attendance record for all employees and all working days
    complete_records = []

    for employee_id in unique_employees:
        employee_worked_days = set(worked_summary[worked_summary["employee_id"] == employee_id]["day"])

        for day in working_days:
            if day in employee_worked_days:
                # Employee worked this day - get the actual record
                work_record = worked_summary[(worked_summary["employee_id"] == employee_id) & (worked_summary["day"] == day)].iloc[0].to_dict()
                complete_records.append(work_record)
            else:
                # Employee was absent this day - create absent record
                absent_record = {
                    "employee_id": employee_id,
                    "day": day,
                    "start_time": None,
                    "end_time": None,
                    "start_device_sn": None,
                    "end_device_sn": None,
                    "time_spent": "0:00:00",
                    "work_status": "absent",
                }
                complete_records.append(absent_record)

    # Create the final summary DataFrame
    summary = pd.DataFrame(complete_records)

    # Sort by employee_id and day
    summary = summary.sort_values(["employee_id", "day"]).reset_index(drop=True)

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
