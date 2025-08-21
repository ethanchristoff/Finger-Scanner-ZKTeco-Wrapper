from datetime import timedelta
from typing import Any

import pandas as pd

from .core.db_queries import get_attendences, get_device_logs, get_finger_log, get_migrations, get_user_shift_mappings, get_users

NOON_HOUR = 12
SUNDAY_WEEKDAY = 6


def fetch_all_data() -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Fetch all required data from the database using helper functions."""
    attendences = get_attendences() or []
    device_logs = get_device_logs() or []
    finger_logs = get_finger_log() or []
    migration_logs = get_migrations() or []
    user_logs = get_users() or []
    return attendences, device_logs, finger_logs, migration_logs, user_logs


def get_device_for_time(group: pd.DataFrame, time_col: str, sn_col: str, which: str) -> Any:
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


def get_shift_mappings() -> dict[str, dict[str, Any]]:
    """Get shift mappings for all users."""
    shift_mappings = get_user_shift_mappings() or []
    shift_df = pd.DataFrame(shift_mappings)
    
    shift_dict = {}
    if not shift_df.empty:
        for _, shift in shift_df.iterrows():
            shift_dict[str(shift["user_id"])] = {
                "shift_start": shift["shift_start"],
                "shift_end": shift["shift_end"],
                "shift_name": shift["shift_name"]
            }
    return shift_dict


def process_late_checkout(employee_data: pd.DataFrame, entry: pd.Series, shift_dict: dict[str, dict[str, Any]]) -> Any:
    """Process late checkout logic for an entry."""
    timestamp = entry["timestamp"]
    day = timestamp.date()
    time_of_day = timestamp.time()
    employee_id = entry["employee_id"]

    if str(employee_id) in shift_dict:
        shift_info = shift_dict[str(employee_id)]
        shift_end = shift_info["shift_end"]

        if isinstance(shift_end, pd.Timedelta):
            total_seconds = int(shift_end.total_seconds())
            hours, remainder = divmod(total_seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            shift_end = pd.Timestamp(f"{hours:02d}:{minutes:02d}:{seconds:02d}").time()

        if time_of_day > shift_end and timestamp.hour < NOON_HOUR:
            previous_day = timestamp.date() - timedelta(days=1)
            prev_day_entries = employee_data[employee_data["timestamp"].dt.date == previous_day]

            if not prev_day_entries.empty:
                day = previous_day

    return day


def process_attendance_entries(df_att: pd.DataFrame, shift_dict: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """Process attendance entries with late checkout logic."""
    processed_entries = []

    for employee_id in df_att["employee_id"].unique():
        employee_data = df_att[df_att["employee_id"] == employee_id].sort_values("timestamp")

        for _, entry in employee_data.iterrows():
            day = process_late_checkout(employee_data, entry, shift_dict)
            
            processed_entries.append({
                "employee_id": entry["employee_id"],
                "timestamp": entry["timestamp"],
                "sn": entry["sn"],
                "day": day
            })

    return processed_entries


def calculate_time_spent_and_flag(row: pd.Series, shift_dict: dict[str, dict[str, Any]]) -> tuple[str, bool, pd.Timestamp]:
    """Calculate time spent and shift flag for a work row."""
    start_time = row["start_time"]
    end_time = row["end_time"]
    num_entries = row["num_entries"]
    day = row["day"]
    employee_id = str(row["employee_id"])

    has_shift = employee_id in shift_dict
    shift_capped = False

    if num_entries == 1:
        end_of_day = pd.Timestamp.combine(day, pd.Timestamp("23:59:59").time())
        end_time = end_of_day

    if has_shift:
        shift_info = shift_dict[employee_id]
        shift_end_time = shift_info["shift_end"]

        if isinstance(shift_end_time, pd.Timedelta):
            total_seconds = int(shift_end_time.total_seconds())
            hours, remainder = divmod(total_seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            shift_end_time = pd.Timestamp(f"{hours:02d}:{minutes:02d}:{seconds:02d}").time()

        shift_end_datetime = pd.Timestamp.combine(day, shift_end_time)

        if end_time > shift_end_datetime:
            if end_time.hour >= NOON_HOUR:
                pass
            else:
                pass

    time_diff = end_time - start_time
    return str(time_diff).split(".")[0], shift_capped, end_time


def generate_complete_records(worked_summary: pd.DataFrame) -> list[dict[str, Any]]:
    """Generate attendance records only for days when employees actually came in."""
    if worked_summary.empty:
        return []

    complete_records = []

    # Only include records where employees actually worked (no absent days)
    for _, row in worked_summary.iterrows():
        complete_records.append(row.to_dict())

    return complete_records


def process_attendance_summary(attendences: list[dict[str, Any]]) -> pd.DataFrame:
    """
    Process the attendences data to create a summary DataFrame.
    """
    df_att = pd.DataFrame(attendences)
    required_cols = {"employee_id", "timestamp", "sn"}
    if not required_cols.issubset(df_att.columns):
        return pd.DataFrame(columns=[
            "employee_id", "day", "start_time", "end_time",
            "start_device_sn", "end_device_sn", "time_spent",
            "work_status", "shift_capped"
        ])

    if df_att.empty:
        return pd.DataFrame(columns=[
            "employee_id", "day", "start_time", "end_time",
            "start_device_sn", "end_device_sn", "time_spent",
            "work_status", "shift_capped"
        ])

    shift_dict = get_shift_mappings()
    df_att["timestamp"] = pd.to_datetime(df_att["timestamp"])
    
    processed_entries = process_attendance_entries(df_att, shift_dict)
    if not processed_entries:
        return pd.DataFrame(columns=[
            "employee_id", "day", "start_time", "end_time",
            "start_device_sn", "end_device_sn", "time_spent",
            "work_status", "shift_capped"
        ])
    
    df_processed = pd.DataFrame(processed_entries)

    worked_summary = (
        df_processed.groupby(["employee_id", "day"])
        .apply(
            lambda g: pd.Series(
                {
                    "start_time": g["timestamp"].min(),
                    "end_time": g["timestamp"].max(),
                    "start_device_sn": get_device_for_time(g, "timestamp", "sn", "min"),
                    "end_device_sn": get_device_for_time(g, "timestamp", "sn", "max"),
                    "work_status": "worked",
                    "num_entries": len(g),
                }
            ),
            include_groups=False
        )
        .reset_index()
    )

    if worked_summary.empty:
        return pd.DataFrame(columns=[
            "employee_id", "day", "start_time", "end_time",
            "start_device_sn", "end_device_sn", "time_spent",
            "work_status", "shift_capped"
        ])

    time_results = worked_summary.apply(
        lambda row: calculate_time_spent_and_flag(row, shift_dict),
        axis=1,
        result_type="expand"
    )
    
    worked_summary["time_spent"] = time_results[0]
    worked_summary["shift_capped"] = time_results[1]
    worked_summary["end_time"] = time_results[2]
    worked_summary = worked_summary.drop(columns=["num_entries"])

    complete_records = generate_complete_records(worked_summary)
    summary = pd.DataFrame(complete_records)
    
    if not summary.empty and "employee_id" in summary.columns and "day" in summary.columns:
        summary = summary.sort_values(["employee_id", "day"]).reset_index(drop=True)

    return summary


def export_to_excel(
    attendences: list[dict[str, Any]],
    device_logs: list[dict[str, Any]],
    finger_logs: list[dict[str, Any]],
    migration_logs: list[dict[str, Any]],
    user_logs: list[dict[str, Any]],
    attendance_summary: pd.DataFrame
) -> None:
    """Export all data and the attendance summary to an Excel file with separate sheets."""
    with pd.ExcelWriter("output.xlsx", engine="openpyxl") as writer:
        pd.DataFrame(attendences).to_excel(writer, sheet_name="Attendences", index=False)
        pd.DataFrame(device_logs).to_excel(writer, sheet_name="DeviceLogs", index=False)
        pd.DataFrame(finger_logs).to_excel(writer, sheet_name="FingerLogs", index=False)
        pd.DataFrame(migration_logs).to_excel(writer, sheet_name="Migrations", index=False)
        pd.DataFrame(user_logs).to_excel(writer, sheet_name="Users", index=False)
        if attendance_summary is not None:
            attendance_summary.to_excel(writer, sheet_name="AttendanceSummary", index=False)


def main(start_date: str | None = None, end_date: str | None = None) -> str:
    """
    Main function to orchestrate data fetching, processing, and exporting.
    Optionally filters attendences by a date range.
    """
    attendences, device_logs, finger_logs, migration_logs, user_logs = fetch_all_data()
    
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
