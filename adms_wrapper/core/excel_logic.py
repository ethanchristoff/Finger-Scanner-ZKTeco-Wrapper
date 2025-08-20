import io

import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import PatternFill

from adms_wrapper.__main__ import process_attendance_summary
from adms_wrapper.core.db_queries import get_device_branch_mappings, get_employee_designation_mappings, get_employee_branch_mappings


def generate_attendance_summary(attendences, device_logs, finger_logs, migration_logs, user_logs, shift_mappings=None):
    summary_df = process_attendance_summary(attendences)
    if summary_df is not None and not summary_df.empty:
        mappings = get_device_branch_mappings() or []
        mappings_df = pd.DataFrame(mappings)

        def map_branch(sn):
            if pd.isna(sn) or sn is None:
                return ""
            row = mappings_df[mappings_df["serial_number"] == str(sn)]
            if not row.empty:
                return row.iloc[0]["branch_name"]
            return ""

        # Only map branch names for worked days (where devices are available)
        summary_df["start_device_sn_branch"] = summary_df.apply(lambda row: map_branch(row["start_device_sn"]) if row["work_status"] == "worked" else "", axis=1)
        summary_df["end_device_sn_branch"] = summary_df.apply(lambda row: map_branch(row["end_device_sn"]) if row["work_status"] == "worked" else "", axis=1)

        # --- Designation mapping ---
        designation_mappings = get_employee_designation_mappings() or []
        designation_df = pd.DataFrame(designation_mappings)

        def map_designation(emp_id):
            if pd.isna(emp_id) or emp_id is None:
                return ""
            row = designation_df[designation_df["employee_id"] == str(emp_id)]
            if not row.empty:
                return row.iloc[0]["designation"]
            return ""

        summary_df["designation"] = summary_df["employee_id"].apply(map_designation)

        # --- Employee branch mapping ---
        employee_branch_mappings = get_employee_branch_mappings() or []
        employee_branch_df = pd.DataFrame(employee_branch_mappings)

        def map_employee_branch(emp_id):
            if pd.isna(emp_id) or emp_id is None:
                return ""
            row = employee_branch_df[employee_branch_df["employee_id"] == str(emp_id)]
            if not row.empty:
                return row.iloc[0]["branch_name"]
            return ""

        summary_df["employee_branch"] = summary_df["employee_id"].apply(map_employee_branch)

        # --- Shift logic ---
        shift_df = pd.DataFrame(shift_mappings) if shift_mappings else pd.DataFrame()

        def get_shift_info(emp_id, work_status, start_time, end_time, shift_capped=False):
            if shift_df.empty:
                return "", "no shift"
            shift_row = shift_df[shift_df["user_id"] == str(emp_id)]
            if shift_row.empty:
                return "", "no shift"

            shift_name = shift_row.iloc[0]["shift_name"]
            shift_start = shift_row.iloc[0]["shift_start"]
            shift_end = shift_row.iloc[0]["shift_end"]

            # For absent days, just return shift info without time comparison
            if work_status == "absent":
                return shift_name, "absent"

            # Check if work hours were capped due to shift constraints
            if shift_capped:
                return shift_name, "shift_capped"

            # Compare times for worked days
            flag = "on time"
            try:
                if pd.notna(start_time) and str(start_time) != "":
                    if str(start_time)[11:16] > str(shift_start):
                        flag = "late in"
                if pd.notna(end_time) and str(end_time) != "":
                    # Check for late checkout scenario
                    end_time_hour = end_time.hour if hasattr(end_time, 'hour') else int(str(end_time)[11:13])
                    end_time_str = str(end_time)[11:16]
                    
                    # If checkout is after shift end but before noon, mark as late checkout
                    if end_time_str > str(shift_end):
                        if end_time_hour < 12:
                            flag = "late checkout"
                        # If after noon, it's considered next day cycle, so normal checkout
                    elif end_time_str < str(shift_end):
                        flag = "early out"
            except Exception:
                pass
            return shift_name, flag

        summary_df["shift_name"] = ""
        summary_df["shift_flag"] = ""
        for idx, row in summary_df.iterrows():
            shift_capped = row.get("shift_capped", False)
            shift_name, flag = get_shift_info(row["employee_id"], row["work_status"], row["start_time"], row["end_time"], shift_capped)
            summary_df.at[idx, "shift_name"] = shift_name
            summary_df.at[idx, "shift_flag"] = flag

        # Create subtotal rows for each employee
        output_rows = []
        for emp_id, group in summary_df.groupby("employee_id", sort=False):
            output_rows.extend(group.to_dict(orient="records"))
            # Calculate subtotal only for worked days
            worked_group = group[group["work_status"] == "worked"]
            if not worked_group.empty:
                worked_group["time_spent_td"] = pd.to_timedelta(worked_group["time_spent"])
                subtotal = worked_group["time_spent_td"].sum()
                subtotal_str = str(subtotal).split(".")[0]
            else:
                subtotal_str = "0:00:00"

            subtotal_row = {col: "" for col in summary_df.columns}
            subtotal_row["employee_id"] = emp_id
            # Get the designation from the first row of the group
            if not group.empty:
                subtotal_row["designation"] = group.iloc[0]["designation"]
            subtotal_row["day"] = "Subtotal"
            subtotal_row["time_spent"] = subtotal_str
            subtotal_row["work_status"] = "subtotal"
            output_rows.append(subtotal_row)
        merged = pd.DataFrame(output_rows, columns=summary_df.columns)
    else:
        merged = summary_df if summary_df is not None else pd.DataFrame()
    return merged


def write_excel(attendences, device_logs, finger_logs, migration_logs, user_logs, merged):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        pd.DataFrame(attendences).to_excel(writer, sheet_name="Attendences", index=False)
        pd.DataFrame(device_logs).to_excel(writer, sheet_name="DeviceLogs", index=False)
        pd.DataFrame(finger_logs).to_excel(writer, sheet_name="FingerLogs", index=False)
        pd.DataFrame(migration_logs).to_excel(writer, sheet_name="Migrations", index=False)
        pd.DataFrame(user_logs).to_excel(writer, sheet_name="Users", index=False)
        merged.to_excel(writer, sheet_name="AttendanceSummary", index=False)
    output.seek(0)
    wb = load_workbook(output)
    # Highlight subtotal rows in blue, worked days green, and absent days red
    if "AttendanceSummary" in wb.sheetnames:
        ws_sum = wb["AttendanceSummary"]
        day_col = None
        work_status_col = None
        shift_capped_col = None
        for idx, cell in enumerate(ws_sum[1], start=1):
            if cell.value == "day":
                day_col = idx
            if cell.value == "work_status":
                work_status_col = idx
            if cell.value == "shift_capped":
                shift_capped_col = idx
        blue_fill = PatternFill(start_color="4F81BD", end_color="4F81BD", fill_type="solid")
        green_fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
        red_fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
        yellow_fill = PatternFill(start_color="FFEB3B", end_color="FFEB3B", fill_type="solid")  # For shift_capped
        for row in ws_sum.iter_rows(min_row=2, max_row=ws_sum.max_row, min_col=1, max_col=ws_sum.max_column):
            if day_col and row[day_col - 1].value == "Subtotal":
                for cell in row:
                    cell.fill = blue_fill
            elif work_status_col:
                work_status = row[work_status_col - 1].value
                shift_capped = row[shift_capped_col - 1].value if shift_capped_col else False
                
                if shift_capped:
                    # Priority for shift_capped highlighting
                    for cell in row:
                        cell.fill = yellow_fill
                elif work_status == "worked":
                    for cell in row:
                        cell.fill = green_fill
                elif work_status == "absent":
                    for cell in row:
                        cell.fill = red_fill
    new_output = io.BytesIO()
    wb.save(new_output)
    new_output.seek(0)
    return new_output
