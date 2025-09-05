import contextlib
from datetime import timedelta
from typing import Any

import pandas as pd

from .db_queries import get_comprehensive_employee_data, get_setting, get_user_shift_mappings

SUNDAY_WEEKDAY = 6


def get_shift_mappings() -> dict[str, dict[str, Any]]:
    """Get shift mappings from the database and format them as a dictionary."""
    mappings = get_user_shift_mappings() or []
    shift_dict = {}

    def _to_time_str(val):
        if val is None:
            return None
        if isinstance(val, timedelta):
            return (pd.Timestamp("1970-01-01") + val).strftime("%H:%M:%S")
        if isinstance(val, (int, float)):
            return (pd.Timestamp("1970-01-01") + timedelta(seconds=float(val))).strftime("%H:%M:%S")
        try:
            return pd.to_datetime(val).strftime("%H:%M:%S")
        except Exception:
            return str(val)

    for mapping in mappings:
        raw_id = mapping.get("user_id") or mapping.get("employee_id") or mapping.get("user") or ""
        if raw_id is None:
            continue
        employee_id = str(raw_id).strip()
        if not employee_id:
            continue

        shift_start = _to_time_str(mapping.get("shift_start"))
        shift_end = _to_time_str(mapping.get("shift_end"))

        shift_dict[employee_id] = {
            "shift_name": mapping.get("shift_name") or "",
            "shift_start": shift_start,
            "shift_end": shift_end,
        }
    return shift_dict


def get_device_for_time(group: pd.DataFrame, time_col: str, device_col: str, operation: str) -> Any:
    """Get device for min/max time in a group."""
    idx = group[time_col].idxmin() if operation == "min" else group[time_col].idxmax()
    return group.loc[idx, device_col]


def is_weekend(date: pd.Timestamp) -> bool:
    """Check if a date is a weekend (Sunday)."""
    return date.weekday() == SUNDAY_WEEKDAY


def calculate_time_spent_and_flag(row: pd.Series, shift_dict: dict[str, dict[str, Any]]) -> tuple[str, bool, bool, pd.Timestamp, str, bool]:
    """
    Calculate time spent and determine shift status flags (late in, early out, no checkout, overtime).

    Returns:
    - time_spent_str: Formatted time string "HH:MM:SS".
    - is_flagged_bool: True if the employee has no checkout or was shift-capped.
    - is_early_checkout_bool: True if the employee checked out before their shift ended.
    - end_time_to_use: The effective end time for the record.
    - shift_flag: String indicating the status of the shift ("normal", "overtime", "late in", "early out").
    - is_late_in_bool: True if the employee checked in after their shift start time + grace period.
    """
    start_time = row["start_time"]
    end_time = row["end_time"]
    employee_id = str(row["employee_id"])
    day = row["day"]

    is_early_checkout = False  # Initialize early checkout flag
    is_late_in = False  # Initialize late in flag
    shift_flag = "normal"  # Default shift flag

    if pd.isna(start_time):
        return "0:00:00", False, is_early_checkout, end_time, "absent", False

    if pd.notna(end_time) and end_time <= start_time:
        end_time += timedelta(days=1)

    def _format_timedelta(td):
        if pd.isna(td) or td < pd.Timedelta(0):
            return "0:00:00"
        total_seconds = int(td.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    day_date = pd.to_datetime(day)
    if is_weekend(day_date):
        if pd.isna(end_time):
            return "0:00:00", True, is_early_checkout, end_time, "weekend", False
        time_diff = end_time - start_time
        return _format_timedelta(time_diff), False, is_early_checkout, end_time, "weekend", False

    # Default shift logic: get employee's specific shift, or fall back to the "default" shift.
    shift_info = shift_dict.get(employee_id) or shift_dict.get("default")

    if shift_info:
        shift_start = pd.to_datetime(shift_info["shift_start"]).time()
        shift_end = pd.to_datetime(shift_info["shift_end"]).time()
        shift_start_dt = pd.to_datetime(f"{day} {shift_start}")
        shift_end_dt = pd.to_datetime(f"{day} {shift_end}")

        if shift_end_dt <= shift_start_dt:
            shift_end_dt += timedelta(days=1)

        try:
            grace_minutes = int(get_setting("late_checkout_grace_minutes") or 15)
            cap_hours = int(get_setting("shift_cap_hours") or 8)
            # Set a default late_in grace period of 15 minutes
            late_in_grace_minutes = 15
        except (ValueError, TypeError):
            grace_minutes = 15
            cap_hours = 8
            late_in_grace_minutes = 15

        # Check for late check-in
        is_late_in = pd.notna(start_time) and start_time > (shift_start_dt + timedelta(minutes=late_in_grace_minutes))

        # Determine overtime cutoff - when a checkout is considered overtime
        overtime_cutoff = shift_end_dt + timedelta(minutes=grace_minutes)

        # Determine cap deadline - when a shift is considered to have no checkout
        cap_deadline = shift_end_dt + timedelta(minutes=grace_minutes) + timedelta(hours=cap_hours)

        is_capped = pd.isna(end_time) or (pd.notna(end_time) and end_time > cap_deadline)
        is_overtime = pd.notna(end_time) and end_time > overtime_cutoff and not is_capped

        try:
            zero_when_capped = get_setting("shift_cap_type")
            should_zero = zero_when_capped.lower() in ("zero")
        except (ValueError, TypeError):
            should_zero = True

        # Determine shift flag based on status (priority: no checkout > early out > late in > overtime > normal)
        early_out = pd.notna(end_time) and end_time < shift_end_dt

        if is_capped:
            shift_flag = "no checkout"
        elif early_out:
            is_early_checkout = True
            shift_flag = "early out"
        elif is_late_in:
            shift_flag = "late in"
        elif is_overtime:
            shift_flag = "overtime"
        else:
            shift_flag = "normal"

        if is_capped:
            # Always set shift_flag to "no checkout" when capped
            shift_flag = "no checkout"
            if should_zero:
                return "0:00:00", True, False, cap_deadline, shift_flag, is_late_in
            else:
                effective_end_time = min(end_time if pd.notna(end_time) else cap_deadline, cap_deadline)
                time_spent_td = effective_end_time - start_time
                return _format_timedelta(time_spent_td), True, False, cap_deadline, shift_flag, is_late_in
        else:
            # Only if not capped can an employee be early.
            if early_out:
                is_early_checkout = True
            time_spent_td = end_time - start_time
            return _format_timedelta(time_spent_td), False, is_early_checkout, end_time, shift_flag, is_late_in

    # Fallback logic for employees without any assigned shift (not even default).
    try:
        cap_hours = int(get_setting("shift_cap_hours") or 8)
    except (ValueError, TypeError):
        cap_hours = 8

    cap_duration = timedelta(hours=cap_hours)
    cap_deadline = start_time + cap_duration
    is_capped = pd.isna(end_time) or (pd.notna(end_time) and (end_time - start_time) > cap_duration)

    try:
        zero_when_capped = get_setting("zero_hours_when_capped") or "true"
        should_zero = zero_when_capped.lower() in ("true", "1", "yes", "on")
    except (ValueError, TypeError):
        should_zero = True

    # For employees without shifts, we can only determine if they're capped
    # Other flags like late-in, overtime don't apply without shift information
    shift_flag = "no checkout" if is_capped else "normal"
    is_late_in = False  # Cannot determine late-in without shift start time

    if is_capped:
        # Ensure shift_flag is "no checkout" for consistency
        shift_flag = "no checkout"
        if should_zero:
            return "0:00:00", True, False, cap_deadline, shift_flag, is_late_in
        effective_end_time = min(end_time if pd.notna(end_time) else cap_deadline, cap_deadline)
        time_spent_duration = effective_end_time - start_time
        return _format_timedelta(time_spent_duration), True, False, cap_deadline, shift_flag, is_late_in

    time_spent_duration = end_time - start_time
    return _format_timedelta(time_spent_duration), False, False, end_time, shift_flag, is_late_in


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
        if start_date and end_date:
            return generate_absent_days_for_date_range(start_date, end_date)
        return []

    if start_date and end_date:
        start_pd = pd.to_datetime(start_date).date()
        end_pd = pd.to_datetime(end_date).date()
        all_days = pd.date_range(start=start_pd, end=end_pd, freq="D").date
    else:
        all_days = pd.date_range(start=worked_summary["day"].min(), end=worked_summary["day"].max(), freq="D").date

    all_days = [day for day in all_days if pd.to_datetime(day).weekday() != SUNDAY_WEEKDAY]
    all_employees = set(worked_summary["employee_id"].unique())
    complete_records = []

    for employee_id in all_employees:
        if not employee_id:
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
                        "early_checkout": False,
                        "shift_flag": "absent",
                        "late_in": False,
                    }
                )

    return complete_records


def generate_absent_days_for_date_range(start_date: str, end_date: str) -> list[dict[str, Any]]:
    """Generate absent day records for all known employees within a specific date range, excluding Sundays."""
    all_employees = get_comprehensive_employee_data() or []
    if not all_employees:
        return []

    start_pd = pd.to_datetime(start_date).date()
    end_pd = pd.to_datetime(end_date).date()
    all_days = pd.date_range(start=start_pd, end=end_pd, freq="D").date
    all_days = [day for day in all_days if pd.to_datetime(day).weekday() != SUNDAY_WEEKDAY]
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
                    "early_checkout": False,
                    "shift_flag": "absent",
                    "late_in": False,
                }
            )
    return absent_records


def _get_absent_days_fallback(start_date: str | None, end_date: str | None) -> pd.DataFrame:
    """Get absent days as fallback when no attendance data is found."""
    if start_date and end_date:
        absent_records = generate_absent_days_for_date_range(start_date, end_date)
        if absent_records:
            return pd.DataFrame(absent_records)

    # Define all columns to avoid errors on empty DataFrame
    return pd.DataFrame(
        columns=["employee_id", "day", "start_time", "end_time", "start_device_sn", "end_device_sn", "time_spent", "work_status", "no_checkout", "early_checkout", "shift_flag", "late_in"]
    )


def process_attendance_summary(attendences: list[dict[str, Any]], start_date: str | None = None, end_date: str | None = None) -> pd.DataFrame:
    """
    Process the attendences data to create a summary DataFrame.
    """
    if not attendences:
        return _get_absent_days_fallback(start_date, end_date)

    df_att = pd.DataFrame(attendences)
    required_cols = {"employee_id", "timestamp", "sn"}

    if not required_cols.issubset(df_att.columns):
        return _get_absent_days_fallback(start_date, end_date)

    df_att["employee_id"] = df_att["employee_id"].apply(lambda x: "" if pd.isna(x) else str(x).strip())
    df_att = df_att[df_att["employee_id"] != ""]

    if df_att.empty:
        return _get_absent_days_fallback(start_date, end_date)

    shift_dict = get_shift_mappings()
    df_att["timestamp"] = pd.to_datetime(df_att["timestamp"])

    worked_rows: list[dict[str, Any]] = []
    for emp, emp_df in df_att.groupby("employee_id"):
        emp_df_sorted = emp_df.sort_values("timestamp").reset_index(drop=True)
        n = len(emp_df_sorted)
        i = 0
        while i < n:
            st_row = emp_df_sorted.loc[i]
            st = st_row["timestamp"]

            boundary_dt = None
            shift_info = shift_dict.get(emp) or shift_dict.get("default")

            if shift_info:
                shift_start_time = pd.to_datetime(shift_info["shift_start"]).time()
                shift_end_time = pd.to_datetime(shift_info["shift_end"]).time()
                shift_start_dt = pd.to_datetime(f"{st.date()} {shift_start_time}")
                shift_end_dt = pd.to_datetime(f"{st.date()} {shift_end_time}")

                if shift_end_dt <= shift_start_dt:
                    shift_end_dt += timedelta(days=1)

                try:
                    cap_hours = int(get_setting("shift_cap_hours") or 8)
                    grace_minutes = int(get_setting("late_checkout_grace_minutes") or 15)
                except Exception:
                    cap_hours = 8
                    grace_minutes = 15

                cap_dt = shift_end_dt + timedelta(minutes=grace_minutes) + timedelta(hours=cap_hours)
                next_shift_start_dt = shift_start_dt + timedelta(days=1)
                boundary_dt = min(cap_dt, next_shift_start_dt)
            else:
                try:
                    cap_hours = int(get_setting("shift_cap_hours") or 8)
                except Exception:
                    cap_hours = 8
                boundary_dt = st + timedelta(hours=cap_hours)

            j = i + 1
            candidate_index = None
            while j < n:
                ts_j = emp_df_sorted.loc[j, "timestamp"]
                if ts_j <= st:
                    j += 1
                    continue
                if boundary_dt and ts_j >= boundary_dt:
                    break
                candidate_index = j
                j += 1

            if candidate_index is not None:
                end_row = emp_df_sorted.loc[candidate_index]
                worked_rows.append(
                    {
                        "employee_id": emp,
                        "day": st.date(),
                        "start_time": st,
                        "end_time": end_row["timestamp"],
                        "start_device_sn": st_row.get("sn", ""),
                        "end_device_sn": end_row.get("sn", ""),
                        "work_status": "worked",
                    }
                )
                i = candidate_index + 1
            else:
                worked_rows.append(
                    {
                        "employee_id": emp,
                        "day": st.date(),
                        "start_time": st,
                        "end_time": pd.NaT,
                        "start_device_sn": st_row.get("sn", ""),
                        "end_device_sn": "",
                        "work_status": "worked",
                    }
                )
                i += 1

    if not worked_rows:
        return _get_absent_days_fallback(start_date, end_date)

    worked_summary = pd.DataFrame(worked_rows)
    time_results = worked_summary.apply(lambda row: calculate_time_spent_and_flag(row, shift_dict), axis=1, result_type="expand")

    worked_summary["time_spent"] = time_results[0]
    worked_summary["no_checkout"] = time_results[1]
    worked_summary["early_checkout"] = time_results[2]
    worked_summary["end_time"] = time_results[3]
    worked_summary["shift_flag"] = time_results[4]  # Add the new shift_flag column
    worked_summary["late_in"] = time_results[5]  # Add the late_in flag column

    complete_records = generate_complete_records(worked_summary, start_date, end_date)
    summary = pd.DataFrame(complete_records)

    if not summary.empty:
        summary = summary.sort_values(["employee_id", "day"]).reset_index(drop=True)

    return summary
