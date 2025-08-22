import io
from typing import Any

import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import PatternFill

from adms_wrapper.core.data_processing import process_attendance_summary
from adms_wrapper.core.db_queries import (
    get_device_branch_mappings,
    get_employee_branch_mappings,
    get_employee_designation_mappings,
    get_employee_name_mappings,
)

NOON_HOUR = 12


def map_branch(sn: Any, mappings_df: pd.DataFrame) -> str:
    """Map device serial number to branch name."""
    if pd.isna(sn) or sn is None:
        return ""
    row = mappings_df[mappings_df["serial_number"] == str(sn)]
    if not row.empty:
        return row.iloc[0]["branch_name"]
    return ""


def map_designation(emp_id: Any, designation_df: pd.DataFrame) -> str:
    """Map employee ID to designation."""
    if pd.isna(emp_id) or emp_id is None or designation_df.empty:
        return ""
    if "employee_id" not in designation_df.columns:
        return ""
    row = designation_df[designation_df["employee_id"] == str(emp_id)]
    if not row.empty:
        return row.iloc[0]["designation"]
    return ""


def map_employee_branch(emp_id: Any, employee_branch_df: pd.DataFrame) -> str:
    """Map employee ID to branch name."""
    if pd.isna(emp_id) or emp_id is None or employee_branch_df.empty:
        return ""
    if "employee_id" not in employee_branch_df.columns:
        return ""
    row = employee_branch_df[employee_branch_df["employee_id"] == str(emp_id)]
    if not row.empty:
        return row.iloc[0]["branch_name"]
    return ""
    return ""


def map_employee_name(emp_id: Any, employee_name_df: pd.DataFrame) -> str:
    """Map employee ID to employee name."""
    if pd.isna(emp_id) or emp_id is None or employee_name_df.empty:
        return ""
    if "employee_id" not in employee_name_df.columns:
        return ""
    row = employee_name_df[employee_name_df["employee_id"] == str(emp_id)]
    if not row.empty:
        return row.iloc[0]["employee_name"]
    return ""


def determine_shift_flag(start_time: Any, end_time: Any, shift_start: Any, shift_end: Any) -> str:
    """Determine shift flag based on times."""
    flag = "on time"
    try:
        # Check for late check-in - now includes exact shift start time as late
        if pd.notna(start_time) and str(start_time) != "" and str(start_time)[11:16] >= str(shift_start):
            flag = "late in"

        if pd.notna(end_time) and str(end_time) != "":
            end_time_hour = end_time.hour if hasattr(end_time, "hour") else int(str(end_time)[11:13])
            end_time_str = str(end_time)[11:16]

            # Check for late checkout - now includes exact shift end time as late
            if end_time_str >= str(shift_end) and end_time_hour < NOON_HOUR:
                flag = "late checkout"
            elif end_time_str < str(shift_end):
                flag = "early out"
    except Exception:
        pass
    return flag


def determine_no_shift_flag(end_time: Any) -> str:
    """Determine shift flag for employees with no shift assignment."""
    flag = "on time"
    try:
        if pd.notna(end_time) and str(end_time) != "":
            end_time_str = str(end_time)[11:16]

            # Check if checkout is exactly at 12:00 or later
            if end_time_str >= "12:00":
                flag = "late checkout"
            elif end_time_str < "12:00":
                flag = "early out"
    except Exception:
        pass
    return flag


def get_shift_info_with_capped(emp_id: str, work_status: str, start_time: Any, end_time: Any, shift_df: pd.DataFrame) -> tuple[str, str]:
    """Get shift information for an employee without shift_capped parameter."""
    if shift_df.empty:
        # No shift data available - use no-shift logic
        if work_status == "absent":
            return "", "absent"
        flag = determine_no_shift_flag(end_time)
        return "", flag

    shift_row = shift_df[shift_df["user_id"] == str(emp_id)]
    if shift_row.empty:
        # Employee has no assigned shift - use no-shift logic
        if work_status == "absent":
            return "", "absent"
        flag = determine_no_shift_flag(end_time)
        return "", flag

    shift_name = shift_row.iloc[0]["shift_name"]
    shift_start = shift_row.iloc[0]["shift_start"]
    shift_end = shift_row.iloc[0]["shift_end"]

    if work_status == "absent":
        return shift_name, "absent"

    flag = determine_shift_flag(start_time, end_time, shift_start, shift_end)
    return shift_name, flag


def apply_branch_mappings(summary_df: pd.DataFrame) -> pd.DataFrame:
    """Apply device branch mappings to summary DataFrame."""
    mappings = get_device_branch_mappings() or []
    mappings_df = pd.DataFrame(mappings)

    summary_df["start_device_sn_branch"] = summary_df.apply(lambda row: map_branch(row["start_device_sn"], mappings_df) if row["work_status"] == "worked" else "", axis=1)
    summary_df["end_device_sn_branch"] = summary_df.apply(lambda row: map_branch(row["end_device_sn"], mappings_df) if row["work_status"] == "worked" else "", axis=1)
    return summary_df


def apply_designation_mappings(summary_df: pd.DataFrame) -> pd.DataFrame:
    """Apply designation mappings to summary DataFrame."""
    designation_mappings = get_employee_designation_mappings() or []
    designation_df = pd.DataFrame(designation_mappings)
    if not summary_df.empty and "employee_id" in summary_df.columns:
        summary_df["designation"] = summary_df["employee_id"].apply(lambda emp_id: map_designation(emp_id, designation_df))
    else:
        summary_df["designation"] = ""
    return summary_df


def apply_employee_branch_mappings(summary_df: pd.DataFrame) -> pd.DataFrame:
    """Apply employee branch mappings to summary DataFrame."""
    employee_branch_mappings = get_employee_branch_mappings() or []
    employee_branch_df = pd.DataFrame(employee_branch_mappings)
    if not summary_df.empty and "employee_id" in summary_df.columns:
        summary_df["employee_branch"] = summary_df["employee_id"].apply(lambda emp_id: map_employee_branch(emp_id, employee_branch_df))
    else:
        summary_df["employee_branch"] = ""
    return summary_df


def apply_employee_name_mappings(summary_df: pd.DataFrame) -> pd.DataFrame:
    """Apply employee name mappings to summary DataFrame."""
    employee_name_mappings = get_employee_name_mappings() or []
    employee_name_df = pd.DataFrame(employee_name_mappings)
    if not summary_df.empty and "employee_id" in summary_df.columns:
        summary_df["employee_name"] = summary_df["employee_id"].apply(lambda emp_id: map_employee_name(emp_id, employee_name_df))
    else:
        summary_df["employee_name"] = ""
    return summary_df


def apply_shift_mappings(summary_df: pd.DataFrame, shift_mappings: list[dict[str, Any]] | None) -> pd.DataFrame:
    """Apply shift mappings to summary DataFrame."""
    shift_df = pd.DataFrame(shift_mappings) if shift_mappings else pd.DataFrame()

    summary_df["shift_name"] = ""
    summary_df["shift_flag"] = ""

    for idx, row in summary_df.iterrows():
        shift_capped = row.get("shift_capped", False)

        if shift_capped:
            shift_name, _ = get_shift_info_with_capped(row["employee_id"], row["work_status"], row["start_time"], row["end_time"], shift_df)
            flag = "shift_capped"
        else:
            shift_name, flag = get_shift_info_with_capped(row["employee_id"], row["work_status"], row["start_time"], row["end_time"], shift_df)

        summary_df.loc[idx, "shift_name"] = shift_name
        summary_df.loc[idx, "shift_flag"] = flag

    return summary_df


def clean_attendance_summary(summary_df: pd.DataFrame) -> pd.DataFrame:
    """Clean attendance summary by removing unwanted columns and formatting times."""
    if summary_df.empty:
        return summary_df

    # Format start_time and end_time to show only time (not date)
    if "start_time" in summary_df.columns:
        summary_df["start_time"] = summary_df["start_time"].apply(lambda x: x.strftime("%H:%M:%S") if pd.notna(x) and hasattr(x, "strftime") else "")

    if "end_time" in summary_df.columns:
        summary_df["end_time"] = summary_df["end_time"].apply(lambda x: x.strftime("%H:%M:%S") if pd.notna(x) and hasattr(x, "strftime") else "")

    # Remove unwanted columns (keeping start_device_sn_branch and end_device_sn_branch)
    columns_to_remove = ["start_device_sn", "end_device_sn", "shift_capped", "designation", "employee_branch"]

    for col in columns_to_remove:
        if col in summary_df.columns:
            summary_df = summary_df.drop(columns=[col])

    return summary_df


def create_subtotal_rows(summary_df: pd.DataFrame) -> list[dict[str, Any]]:
    """Create subtotal rows for each employee with separate days worked and total hours columns."""
    output_rows = []

    for emp_id, group in summary_df.groupby("employee_id", sort=False):
        output_rows.extend(group.to_dict(orient="records"))

        worked_group = group[group["work_status"] == "worked"].copy()
        days_worked = len(worked_group)

        if not worked_group.empty:
            worked_group.loc[:, "time_spent_td"] = pd.to_timedelta(worked_group["time_spent"])
            subtotal = worked_group["time_spent_td"].sum()
            subtotal_str = str(subtotal).split(".")[0]
        else:
            subtotal_str = "0:00:00"

        subtotal_row = dict.fromkeys(summary_df.columns, "")
        subtotal_row["employee_id"] = emp_id
        if not group.empty:
            subtotal_row["employee_name"] = group.iloc[0].get("employee_name", "")
        subtotal_row["day"] = "Subtotal"
        subtotal_row["days_worked"] = days_worked
        subtotal_row["total_hours"] = subtotal_str
        subtotal_row["work_status"] = "subtotal"
        output_rows.append(subtotal_row)

    return output_rows


def generate_attendance_summary(
    attendences: list[dict[str, Any]],
    _device_logs: list[dict[str, Any]],
    _finger_logs: list[dict[str, Any]],
    _migration_logs: list[dict[str, Any]],
    _user_logs: list[dict[str, Any]],
    shift_mappings: list[dict[str, Any]] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Generate attendance summary with all mappings applied."""
    summary_df = process_attendance_summary(attendences, start_date, end_date)

    if summary_df is not None and not summary_df.empty:
        summary_df = apply_branch_mappings(summary_df)
        summary_df = apply_designation_mappings(summary_df)
        summary_df = apply_employee_branch_mappings(summary_df)
        summary_df = apply_employee_name_mappings(summary_df)
        summary_df = apply_shift_mappings(summary_df, shift_mappings)

        # Clean the summary before creating subtotal rows
        summary_df = clean_attendance_summary(summary_df)

        output_rows = create_subtotal_rows(summary_df)
        merged = pd.DataFrame(output_rows, columns=[*summary_df.columns.tolist(), "days_worked", "total_hours"])
    else:
        merged = summary_df if summary_df is not None else pd.DataFrame()

    return merged


def find_column_indices(ws: Any) -> tuple[int | None, int | None, int | None]:
    """Find column indices for highlighting."""
    day_col = None
    work_status_col = None
    shift_capped_col = None

    for idx, cell in enumerate(ws[1], start=1):
        if cell.value == "day":
            day_col = idx
        if cell.value == "work_status":
            work_status_col = idx
        if cell.value == "shift_capped":
            shift_capped_col = idx

    return day_col, work_status_col, shift_capped_col


def apply_subtotal_highlighting(row: list, day_col: int | None, blue_fill: PatternFill) -> None:
    """Apply highlighting for subtotal rows."""
    if day_col and row[day_col - 1].value == "Subtotal":
        for cell in row:
            cell.fill = blue_fill


def apply_status_highlighting(row: list, work_status_col: int | None, shift_capped_col: int | None, green_fill: PatternFill, red_fill: PatternFill, yellow_fill: PatternFill) -> None:
    """Apply highlighting based on work status."""
    if not work_status_col:
        return

    work_status = row[work_status_col - 1].value
    shift_capped = row[shift_capped_col - 1].value if shift_capped_col else False

    if shift_capped:
        for cell in row:
            cell.fill = yellow_fill
    elif work_status == "worked":
        for cell in row:
            cell.fill = green_fill
    elif work_status == "absent":
        for cell in row:
            cell.fill = red_fill


def apply_row_highlighting(ws: Any) -> None:
    """Apply highlighting to rows based on work status."""
    day_col, work_status_col, shift_capped_col = find_column_indices(ws)

    blue_fill = PatternFill(start_color="4F81BD", end_color="4F81BD", fill_type="solid")
    green_fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
    red_fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
    yellow_fill = PatternFill(start_color="FFEB3B", end_color="FFEB3B", fill_type="solid")

    for row in ws.iter_rows(min_row=2, max_row=ws.max_row, min_col=1, max_col=ws.max_column):
        apply_subtotal_highlighting(row, day_col, blue_fill)
        apply_status_highlighting(row, work_status_col, shift_capped_col, green_fill, red_fill, yellow_fill)


def create_employee_summary_sheet(summary_df: pd.DataFrame) -> pd.DataFrame:
    """Create a comprehensive summary sheet with employee statistics including days worked, subtotal hours, etc."""
    if summary_df.empty:
        return pd.DataFrame()

    summary_rows = []

    for emp_id, group in summary_df.groupby("employee_id", sort=False):
        # Get employee info from the first row of the group
        first_row = group.iloc[0]

        # Calculate statistics
        total_days = len(group)
        worked_days = len(group[group["work_status"] == "worked"])
        absent_days = len(group[group["work_status"] == "absent"])

        # Calculate total worked hours
        worked_group = group[group["work_status"] == "worked"].copy()
        if not worked_group.empty:
            worked_group.loc[:, "time_spent_td"] = pd.to_timedelta(worked_group["time_spent"])
            subtotal = worked_group["time_spent_td"].sum()
            subtotal_str = str(subtotal).split(".")[0]

            # Calculate average hours per worked day
            avg_hours_per_day = subtotal / worked_days if worked_days > 0 else pd.Timedelta(0)
            avg_hours_str = str(avg_hours_per_day).split(".")[0]
        else:
            subtotal_str = "0:00:00"
            avg_hours_str = "0:00:00"

        # Get shift information
        shift_name = first_row.get("shift_name", "")

        # Count different shift flags
        late_in_count = len(group[group["shift_flag"] == "late in"])
        early_out_count = len(group[group["shift_flag"] == "early out"])
        late_checkout_count = len(group[group["shift_flag"] == "late checkout"])
        on_time_count = len(group[group["shift_flag"] == "on time"])
        shift_capped_count = len(group[group["shift_flag"] == "shift_capped"])

        # Calculate attendance percentage
        attendance_percentage = (worked_days / total_days * 100) if total_days > 0 else 0

        # Create summary row
        summary_row = {
            "employee_id": emp_id,
            "employee_name": first_row.get("employee_name", ""),
            "shift_name": shift_name,
            "total_days": total_days,
            "days_worked": worked_days,
            "days_absent": absent_days,
            "attendance_percentage": f"{attendance_percentage:.1f}%",
            "total_hours_worked": subtotal_str,
            "avg_hours_per_day": avg_hours_str,
            "on_time_days": on_time_count,
            "late_in_days": late_in_count,
            "early_out_days": early_out_count,
            "late_checkout_days": late_checkout_count,
            "shift_capped_days": shift_capped_count,
        }
        summary_rows.append(summary_row)

    # Create DataFrame and sort by employee_id
    summary_summary_df = pd.DataFrame(summary_rows)
    if not summary_summary_df.empty:
        summary_summary_df = summary_summary_df.sort_values("employee_id")

    return summary_summary_df


def write_excel(
    attendences: list[dict[str, Any]], device_logs: list[dict[str, Any]], finger_logs: list[dict[str, Any]], migration_logs: list[dict[str, Any]], user_logs: list[dict[str, Any]], merged: pd.DataFrame
) -> io.BytesIO:
    """Write data to Excel file with formatting."""
    output = io.BytesIO()

    # Generate employee summary sheet
    employee_summary = create_employee_summary_sheet(merged)

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        pd.DataFrame(attendences).to_excel(writer, sheet_name="Attendences", index=False)
        pd.DataFrame(device_logs).to_excel(writer, sheet_name="DeviceLogs", index=False)
        pd.DataFrame(finger_logs).to_excel(writer, sheet_name="FingerLogs", index=False)
        pd.DataFrame(migration_logs).to_excel(writer, sheet_name="Migrations", index=False)
        pd.DataFrame(user_logs).to_excel(writer, sheet_name="Users", index=False)
        merged.to_excel(writer, sheet_name="AttendanceSummary", index=False)
        employee_summary.to_excel(writer, sheet_name="EmployeeSummary", index=False)

    output.seek(0)
    wb = load_workbook(output)

    if "AttendanceSummary" in wb.sheetnames:
        apply_row_highlighting(wb["AttendanceSummary"])

    new_output = io.BytesIO()
    wb.save(new_output)
    new_output.seek(0)
    return new_output
