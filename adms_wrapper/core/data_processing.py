from datetime import timedelta
from typing import Any

import pandas as pd

from .db_queries import get_user_shift_mappings, get_comprehensive_employee_data

NOON_HOUR = 12
SUNDAY_WEEKDAY = 6


def get_shift_mappings() -> dict[str, dict[str, Any]]:
    """Get shift mappings from the database and format them as a dictionary."""
    mappings = get_user_shift_mappings() or []
    shift_dict = {}
    for mapping in mappings:
        shift_dict[mapping["user_id"]] = {
            "shift_name": mapping["shift_name"],
            "shift_start": mapping["shift_start"],
            "shift_end": mapping["shift_end"],
        }
    return shift_dict


def get_device_for_time(group: pd.DataFrame, time_col: str, device_col: str, operation: str) -> Any:
    """Get device for min/max time in a group."""
    if operation == "min":
        idx = group[time_col].idxmin()
    else:
        idx = group[time_col].idxmax()
    return group.loc[idx, device_col]


def is_weekend(date: pd.Timestamp) -> bool:
    """Check if a date is a weekend (Sunday)."""
    return date.weekday() == SUNDAY_WEEKDAY


def calculate_time_spent_and_flag(row: pd.Series, shift_dict: dict[str, dict[str, Any]]) -> tuple[str, bool, pd.Timestamp]:
    """Calculate time spent and determine if there's a no-checkout.

    Returns: (time_spent_str, no_checkout_bool, end_time_to_use)
    - no_checkout_bool: True when employee did not checkout between shift_end+15min and next shift start.
    """
    start_time = row["start_time"]
    end_time = row["end_time"]
    employee_id = row["employee_id"]
    day = row["day"]

    # If there's no start time, nothing to compute
    if pd.isna(start_time):
        return "0:00:00", False, end_time

    # end_time may be NaT (no checkout) — handle below depending on shift presence
    time_diff = None
    if pd.notna(end_time):
        time_diff = end_time - start_time

    # Check if it's a weekend
    day_date = pd.to_datetime(day)
    if is_weekend(day_date):
        time_spent_str = str(time_diff).split(".")[0]
        return time_spent_str, False, end_time

    # Check if employee has a shift
    if employee_id in shift_dict:
        shift_info = shift_dict[employee_id]
        shift_start = pd.to_datetime(shift_info["shift_start"]).time()
        shift_end = pd.to_datetime(shift_info["shift_end"]).time()

        # Convert shift times to datetime for the specific day
        shift_start_dt = pd.to_datetime(f"{day} {shift_start}")
        shift_end_dt = pd.to_datetime(f"{day} {shift_end}")

        # If shift_end is not after shift_start, it crosses midnight -> roll end to next day
        if shift_end_dt <= shift_start_dt:
            shift_end_dt = shift_end_dt + timedelta(days=1)

        # Calculate expected shift duration and the next shift start (assume next day's shift_start)
        expected_duration = shift_end_dt - shift_start_dt
        next_shift_start_dt = shift_start_dt + timedelta(days=1)
        # grace deadline after shift end (15 minutes)
        grace_dt = shift_end_dt + timedelta(minutes=15)

        # If there's no recorded checkout (end_time is NaT), treat as no_checkout
        # and set end_time to the grace deadline for time counting (employee didn't checkout)
        if pd.isna(end_time):
            # If start_time is after grace_dt for some odd reason, count from start_time to start_time (0)
            end_use = grace_dt if grace_dt > start_time else start_time
            time_spent_td = end_use - start_time
            time_spent_str = str(time_spent_td).split(".")[0]
            return time_spent_str, True, end_use

        # If end_time is earlier than or equal to start_time, assume it's on the next day
        if end_time <= start_time:
            try:
                end_time = end_time + timedelta(days=1)
                time_diff = end_time - start_time
            except Exception:
                # if adjustment fails, leave as-is
                pass
        # If employee checked out after shift_end_dt
        if end_time > shift_end_dt:
            # Count every minute up to actual checkout as part of shift (late checkout)
            time_spent_td = time_diff if time_diff is not None else (end_time - start_time)
            time_spent_str = str(time_spent_td).split(".")[0]
            return time_spent_str, False, end_time

        # Otherwise, the employee checked out before or at shift end — normal/early out
        time_spent_td = time_diff if time_diff is not None else (end_time - start_time)
        time_spent_str = str(time_spent_td).split(".")[0]
        return time_spent_str, False, end_time
    else:
        # No shift assigned - apply 8-hour cap
        eight_hours = timedelta(hours=8)
        # If there's no end_time, treat as no_checkout and count up to start_time + 8h window
        if pd.isna(end_time):
            cap_dt = start_time + eight_hours
            # Use cap_dt as conservative counting endpoint when no shift is known
            time_spent_str = str(eight_hours).split(".")[0]
            return time_spent_str, True, cap_dt

        # If end_time appears to be earlier than start_time (overnight), roll forward one day
        if end_time <= start_time:
            try:
                end_time = end_time + timedelta(days=1)
                time_diff = end_time - start_time
            except Exception:
                pass

        if time_diff and time_diff > eight_hours:
            # Actual checkout exists beyond 8 hours: count full worked time and do not mark as shift_capped
            time_spent_str = str(time_diff).split(".")[0]
            return time_spent_str, False, end_time

        time_spent_str = str(time_diff).split(".")[0]
        return time_spent_str, False, end_time


def process_attendance_entries(df_att: pd.DataFrame, shift_dict: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """Process attendance entries and return processed list."""
    df_att["day"] = df_att["timestamp"].dt.date
    processed = []

    for _, row in df_att.iterrows():
        processed.append(
            {
                "employee_id": row["employee_id"],
                "timestamp": row["timestamp"],
                "day": row["day"],
                "sn": row["sn"],
            }
        )

    return processed


def generate_complete_records(worked_summary: pd.DataFrame, start_date: str | None = None, end_date: str | None = None) -> list[dict[str, Any]]:
    """Generate complete records including absent days for the full date range, excluding Sundays."""
    if worked_summary.empty:
        # If no worked data but we have date range, generate all absent days
        if start_date and end_date:
            return generate_absent_days_for_date_range(start_date, end_date)
        return []

    # Determine the date range - use provided dates or default to worked data range
    if start_date and end_date:
        start_pd = pd.to_datetime(start_date).date()
        end_pd = pd.to_datetime(end_date).date()
        all_days = pd.date_range(start=start_pd, end=end_pd, freq="D").date
    else:
        # Fallback to the range of worked data
        all_days = pd.date_range(start=worked_summary["day"].min(), end=worked_summary["day"].max(), freq="D").date

    # Filter out Sundays (weekday 6)
    all_days = [day for day in all_days if pd.to_datetime(day).weekday() != 6]

    worked_employees = set(worked_summary["employee_id"].unique())

    all_employees = worked_employees

    complete_records = []

    for employee_id in all_employees:
        if not employee_id:  # Skip empty employee IDs
            continue

        employee_data = worked_summary[worked_summary["employee_id"] == employee_id]

        for day in all_days:
            day_data = employee_data[employee_data["day"] == day]

            if not day_data.empty:
                complete_records.extend(day_data.to_dict(orient="records"))
            else:
                complete_records.append(
                    {
                        "employee_id": employee_id,
                        "day": day,
                        "start_time": pd.NaT,
                        "end_time": pd.NaT,
                        "start_device_sn": "",
                        "end_device_sn": "",
                        "time_spent": "0:00:00",
                        "work_status": "absent",
                        "no_checkout": False,
                    }
                )

    return complete_records


def generate_absent_days_for_date_range(start_date: str, end_date: str) -> list[dict[str, Any]]:
    """Generate absent day records for all known employees within a specific date range, excluding Sundays."""
    # Get all known employees from the system
    all_employees = get_comprehensive_employee_data() or []

    if not all_employees:
        return []

    # Parse date range
    start_pd = pd.to_datetime(start_date).date()
    end_pd = pd.to_datetime(end_date).date()
    all_days = pd.date_range(start=start_pd, end=end_pd, freq="D").date

    # Filter out Sundays (weekday 6)
    all_days = [day for day in all_days if pd.to_datetime(day).weekday() != 6]

    absent_records = []

    for employee in all_employees:
        employee_id = employee.get("employee_id", "")
        if not employee_id:
            continue

        for day in all_days:
            absent_records.append(
                {
                    "employee_id": employee_id,
                    "day": day,
                    "start_time": pd.NaT,
                    "end_time": pd.NaT,
                    "start_device_sn": "",
                    "end_device_sn": "",
                    "time_spent": "0:00:00",
                    "work_status": "absent",
                        "no_checkout": False,
                }
            )

    return absent_records


def _get_absent_days_fallback(start_date: str | None, end_date: str | None) -> pd.DataFrame:
    """Get absent days as fallback when no attendance data is found."""
    if start_date and end_date:
        absent_records = generate_absent_days_for_date_range(start_date, end_date)
        if absent_records:
            return pd.DataFrame(absent_records)
    return pd.DataFrame(columns=["employee_id", "day", "start_time", "end_time", "start_device_sn", "end_device_sn", "time_spent", "work_status", "no_checkout"])


def process_attendance_summary(attendences: list[dict[str, Any]], start_date: str | None = None, end_date: str | None = None) -> pd.DataFrame:
    """
    Process the attendences data to create a summary DataFrame.
    If start_date and end_date are provided and no attendance data exists,
    generate absent day records for all known employees in that date range.
    """
    df_att = pd.DataFrame(attendences)
    required_cols = {"employee_id", "timestamp", "sn"}

    # Check if data has required columns
    if not required_cols.issubset(df_att.columns):
        return _get_absent_days_fallback(start_date, end_date)

    # Check if data is empty
    if df_att.empty:
        return _get_absent_days_fallback(start_date, end_date)

    shift_dict = get_shift_mappings()
    df_att["timestamp"] = pd.to_datetime(df_att["timestamp"])

    processed_entries = process_attendance_entries(df_att, shift_dict)
    if not processed_entries:
        return _get_absent_days_fallback(start_date, end_date)

    df_processed = pd.DataFrame(processed_entries)

    # Build worked summary by pairing timestamps sequentially per employee.
    # This ensures that if a checkout falls beyond the cap window it will NOT be
    # linked back to the previous start; instead that later timestamp will remain
    # available to be treated as a start for the next shift.
    worked_rows: list[dict[str, Any]] = []

    # Prepare per-employee sorted lists of timestamps
    for emp, emp_df in df_processed.groupby("employee_id"):
        emp_df_sorted = emp_df.sort_values("timestamp").reset_index(drop=True)
        n = len(emp_df_sorted)
        i = 0
        while i < n:
            st_row = emp_df_sorted.loc[i]
            st = st_row["timestamp"]
            # Determine pairing boundary for this start.
            # For employees with an assigned shift, we consider any checkout before the next shift start
            # as a checkout for this shift (including late checkouts). If no checkout exists before
            # the next shift start, it will be treated as a no_checkout.
            if emp in shift_dict:
                shift_info = shift_dict[emp]
                shift_start_time = pd.to_datetime(shift_info["shift_start"]).time()
                shift_end_time = pd.to_datetime(shift_info["shift_end"]).time()
                shift_start_dt = pd.to_datetime(f"{st.date()} {shift_start_time}")
                shift_end_dt = pd.to_datetime(f"{st.date()} {shift_end_time}")
                # If shift_end is not after shift_start, it crosses midnight -> roll end to next day
                if shift_end_dt <= shift_start_dt:
                    shift_end_dt = shift_end_dt + timedelta(days=1)
                # Next shift start is shift_start on the following day
                next_shift_start_dt = shift_start_dt + timedelta(days=1)
                boundary_dt = next_shift_start_dt
            else:
                # For employees without a shift mapping, allow any subsequent timestamp to be considered a checkout
                boundary_dt = None

            # Find latest candidate after st and before boundary_dt (if set) or any next timestamp otherwise
            j = i + 1
            candidate_index = None
            while j < n:
                ts_j = emp_df_sorted.loc[j, "timestamp"]
                if ts_j <= st:
                    j += 1
                    continue
                if boundary_dt is not None and ts_j >= boundary_dt:
                    # This timestamp belongs to the next shift window; stop searching
                    break
                # keep the latest found before the boundary
                candidate_index = j
                j += 1

            if candidate_index is not None:
                end_row = emp_df_sorted.loc[candidate_index]
                end_ts = end_row["timestamp"]
                worked_rows.append(
                    {
                        "employee_id": emp,
                        "day": st.date(),
                        "start_time": st,
                        "end_time": end_ts,
                        "start_device_sn": st_row.get("sn", ""),
                        "end_device_sn": end_row.get("sn", ""),
                        "work_status": "worked",
                        "num_entries": candidate_index - i + 1,
                    }
                )
                # consume up to candidate_index
                i = candidate_index + 1
            else:
                # No checkout within cap window -> leave end_time as NaT so cap logic will apply later
                worked_rows.append(
                    {
                        "employee_id": emp,
                        "day": st.date(),
                        "start_time": st,
                        "end_time": pd.NaT,
                        "start_device_sn": st_row.get("sn", ""),
                        "end_device_sn": "",
                        "work_status": "worked",
                        "num_entries": 1,
                    }
                )
                # move to next timestamp which will be treated as next shift's start
                i += 1

    # Create DataFrame from worked_rows
    worked_summary = pd.DataFrame(worked_rows) if worked_rows else pd.DataFrame()

    if worked_summary.empty:
        return _get_absent_days_fallback(start_date, end_date)

    if worked_summary.empty:
        return _get_absent_days_fallback(start_date, end_date)

    time_results = worked_summary.apply(lambda row: calculate_time_spent_and_flag(row, shift_dict), axis=1, result_type="expand")

    worked_summary["time_spent"] = time_results[0]
    # second column now represents 'no_checkout' boolean
    worked_summary["no_checkout"] = time_results[1]
    worked_summary["end_time"] = time_results[2]
    if "num_entries" in worked_summary.columns:
        worked_summary = worked_summary.drop(columns=["num_entries"])

    complete_records = generate_complete_records(worked_summary, start_date, end_date)
    summary = pd.DataFrame(complete_records)

    if not summary.empty and "employee_id" in summary.columns and "day" in summary.columns:
        summary = summary.sort_values(["employee_id", "day"]).reset_index(drop=True)

    return summary
