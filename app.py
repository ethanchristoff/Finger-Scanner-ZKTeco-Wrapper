import os
from typing import Any

import pandas as pd
from flask import Flask, flash, redirect, render_template, request, send_file, url_for

from adms_wrapper.__main__ import process_attendance_summary
from adms_wrapper.core.db_queries import (
    add_comprehensive_employee,
    add_device_branch_mapping,
    add_employee_branch_mapping,
    add_employee_designation_mapping,
    add_employee_name_mapping,
    add_shift_template,
    add_user_shift_mapping,
    assign_shift_template_to_user,
    delete_comprehensive_employee,
    delete_device_branch_mapping,
    delete_employee_branch_mapping,
    delete_employee_designation_mapping,
    delete_employee_name_mapping,
    delete_shift_template,
    delete_user_shift_mapping,
    get_attendences,
    get_comprehensive_employee_data,
    get_device_branch_mappings,
    get_device_logs,
    get_employee_branch_mappings,
    get_employee_designation_mappings,
    get_employee_name_mappings,
    get_finger_log,
    get_migrations,
    get_shift_templates,
    get_user_shift_mappings,
    get_users,
)
from adms_wrapper.core.excel_logic import generate_attendance_summary, write_excel

app = Flask(__name__)
app.secret_key = "your_secret_key_here"


def handle_shift_mapping_deletion(delete_user_id: str) -> None:
    """Handle deletion of user shift mapping."""
    delete_user_shift_mapping(delete_user_id)
    flash(f"Shift mapping deleted: {delete_user_id}", "success")


def handle_shift_mapping_addition(user_id: str, shift_name: str, shift_start: str, shift_end: str) -> None:
    """Handle addition of user shift mapping."""
    if user_id and shift_name and shift_start and shift_end:
        add_user_shift_mapping(user_id, shift_name, shift_start, shift_end)
        flash(f"Shift mapping added: {user_id} → {shift_name}", "success")
    else:
        flash("All fields are required.", "error")


@app.route("/user_shift_mapping", methods=["GET", "POST"])
def user_shift_mapping() -> Any:
    if request.method == "POST":
        delete_user_id = request.form.get("delete_user_id")
        if delete_user_id:
            handle_shift_mapping_deletion(delete_user_id)
            return redirect(url_for("user_shift_mapping"))
        
        user_id = request.form.get("user_id")
        shift_name = request.form.get("shift_name")
        shift_start = request.form.get("shift_start")
        shift_end = request.form.get("shift_end")
        
        handle_shift_mapping_addition(user_id, shift_name, shift_start, shift_end)
        return redirect(url_for("user_shift_mapping"))
    
    mappings = get_user_shift_mappings() or []
    return render_template("user_shift_mapping.html", mappings=mappings)


def handle_device_mapping_deletion(delete_sn: str) -> None:
    """Handle deletion of device branch mapping."""
    delete_device_branch_mapping(delete_sn)
    flash(f"Mapping deleted: {delete_sn}", "success")


def handle_device_mapping_addition(serial_number: str, branch_name: str) -> None:
    """Handle addition of device branch mapping."""
    if serial_number and branch_name:
        add_device_branch_mapping(serial_number, branch_name)
        flash(f"Mapping added: {serial_number} → {branch_name}", "success")
    else:
        flash("Both serial number and branch name are required.", "error")


@app.route("/device_branch_mapping", methods=["GET", "POST"])
def device_branch_mapping() -> Any:
    if request.method == "POST":
        delete_sn = request.form.get("delete_serial")
        if delete_sn:
            handle_device_mapping_deletion(delete_sn)
            return redirect(url_for("device_branch_mapping"))
        
        serial_number = request.form.get("serial_number")
        branch_name = request.form.get("branch_name")
        
        handle_device_mapping_addition(serial_number, branch_name)
        return redirect(url_for("device_branch_mapping"))
    
    mappings = get_device_branch_mappings() or []
    return render_template("device_branch_mapping.html", mappings=mappings)


def handle_designation_mapping_deletion(delete_emp_id: str) -> None:
    """Handle deletion of employee designation mapping."""
    delete_employee_designation_mapping(delete_emp_id)
    flash(f"Designation mapping deleted: {delete_emp_id}", "success")


def handle_designation_mapping_addition(employee_id: str, designation: str) -> None:
    """Handle addition of employee designation mapping."""
    if employee_id and designation:
        add_employee_designation_mapping(employee_id, designation)
        flash(f"Designation mapping added: {employee_id} → {designation}", "success")
    else:
        flash("Both employee ID and designation are required.", "error")


@app.route("/employee_designation_mapping", methods=["GET", "POST"])
def employee_designation_mapping() -> Any:
    if request.method == "POST":
        delete_emp_id = request.form.get("delete_employee_id")
        if delete_emp_id:
            handle_designation_mapping_deletion(delete_emp_id)
            return redirect(url_for("employee_designation_mapping"))
        
        employee_id = request.form.get("employee_id")
        designation = request.form.get("designation")
        
        handle_designation_mapping_addition(employee_id, designation)
        return redirect(url_for("employee_designation_mapping"))
    
    mappings = get_employee_designation_mappings() or []
    return render_template("employee_designation_mapping.html", mappings=mappings)


def handle_employee_name_deletion(delete_emp_id: str) -> None:
    """Handle deletion of employee name mapping."""
    delete_employee_name_mapping(delete_emp_id)
    flash(f"Name mapping deleted: {delete_emp_id}", "success")


def handle_employee_name_addition(employee_id: str, employee_name: str) -> None:
    """Handle addition of employee name mapping."""
    if employee_id and employee_name:
        add_employee_name_mapping(employee_id, employee_name)
        flash(f"Name mapping added: {employee_id} → {employee_name}", "success")
    else:
        flash("Both employee ID and employee name are required.", "error")


@app.route("/employee_name_mapping", methods=["GET", "POST"])
def employee_name_mapping() -> Any:
    if request.method == "POST":
        delete_emp_id = request.form.get("delete_employee_id")
        if delete_emp_id:
            handle_employee_name_deletion(delete_emp_id)
            return redirect(url_for("employee_name_mapping"))
        
        employee_id = request.form.get("employee_id")
        employee_name = request.form.get("employee_name")
        
        handle_employee_name_addition(employee_id, employee_name)
        return redirect(url_for("employee_name_mapping"))
    
    mappings = get_employee_name_mappings() or []
    return render_template("employee_name_mapping.html", mappings=mappings)


def handle_employee_branch_deletion(delete_emp_id: str) -> None:
    """Handle deletion of employee branch mapping."""
    delete_employee_branch_mapping(delete_emp_id)
    flash(f"Branch mapping deleted: {delete_emp_id}", "success")


def handle_employee_branch_addition(employee_id: str, branch_name: str) -> None:
    """Handle addition of employee branch mapping."""
    if employee_id and branch_name:
        add_employee_branch_mapping(employee_id, branch_name)
        flash(f"Branch mapping added: {employee_id} → {branch_name}", "success")
    else:
        flash("Both employee ID and branch name are required.", "error")


@app.route("/employee_branch_mapping", methods=["GET", "POST"])
def employee_branch_mapping() -> Any:
    if request.method == "POST":
        delete_emp_id = request.form.get("delete_employee_id")
        if delete_emp_id:
            handle_employee_branch_deletion(delete_emp_id)
            return redirect(url_for("employee_branch_mapping"))
        
        employee_id = request.form.get("employee_id")
        branch_name = request.form.get("branch_name")
        
        handle_employee_branch_addition(employee_id, branch_name)
        return redirect(url_for("employee_branch_mapping"))
    
    mappings = get_employee_branch_mappings() or []
    all_branches = list({b["branch_name"] for b in get_device_branch_mappings() or []})
    return render_template("employee_branch_mapping.html", mappings=mappings, all_branches=all_branches)


@app.route("/shift_templates", methods=["GET", "POST"])
def shift_templates() -> Any:
    """Manage shift templates."""
    if request.method == "POST":
        delete_shift_name = request.form.get("delete_shift_name")
        if delete_shift_name:
            delete_shift_template(delete_shift_name)
            flash(f"Shift template deleted: {delete_shift_name}", "success")
            return redirect(url_for("shift_templates"))
        
        shift_name = request.form.get("shift_name")
        shift_start = request.form.get("shift_start")
        shift_end = request.form.get("shift_end")
        description = request.form.get("description", "")
        
        if shift_name and shift_start and shift_end:
            add_shift_template(shift_name, shift_start, shift_end, description)
            flash(f"Shift template added: {shift_name}", "success")
        else:
            flash("Shift name, start time, and end time are required.", "error")
        
        return redirect(url_for("shift_templates"))
    
    templates = get_shift_templates() or []
    return render_template("shift_templates.html", templates=templates)


@app.route("/employee_management", methods=["GET", "POST"])
def employee_management() -> Any:
    """Comprehensive employee management."""
    if request.method == "POST":
        delete_emp_id = request.form.get("delete_employee_id")
        if delete_emp_id:
            delete_comprehensive_employee(delete_emp_id)
            flash(f"Employee data deleted: {delete_emp_id}", "success")
            return redirect(url_for("employee_management"))
        
        employee_id = request.form.get("employee_id")
        employee_name = request.form.get("employee_name", "")
        designation = request.form.get("designation", "")
        branch_name = request.form.get("branch_name", "")
        shift_name = request.form.get("shift_name", "")
        
        if employee_id:
            add_comprehensive_employee(employee_id, employee_name, designation, branch_name, shift_name)
            flash(f"Employee data updated: {employee_id}", "success")
        else:
            flash("Employee ID is required.", "error")
        
        return redirect(url_for("employee_management"))
    
    employees = get_comprehensive_employee_data() or []
    all_branches = sorted({b["branch_name"] for b in get_device_branch_mappings() or []})
    all_designations = sorted({d["designation"] for d in get_employee_designation_mappings() or []})
    all_employee_names = sorted({n["employee_name"] for n in get_employee_name_mappings() or []})
    shift_templates = get_shift_templates() or []
    
    return render_template("employee_management.html", 
                         employees=employees,
                         all_branches=all_branches,
                         all_designations=all_designations,
                         all_employee_names=all_employee_names,
                         shift_templates=shift_templates)


@app.route("/unified_management", methods=["GET", "POST"])
def unified_management() -> Any:
    """Unified management interface for all system entities."""
    if request.method == "POST":
        action = request.form.get("action")
        
        if action == "employee":
            # Handle employee management
            delete_emp_id = request.form.get("delete_employee_id")
            if delete_emp_id:
                delete_comprehensive_employee(delete_emp_id)
                flash(f"Employee deleted: {delete_emp_id}", "success")
                return redirect(url_for("unified_management"))
            
            employee_id = request.form.get("employee_id")
            employee_name = request.form.get("employee_name", "")
            designation = request.form.get("designation", "")
            branch_name = request.form.get("branch_name", "")
            shift_name = request.form.get("shift_name", "")
            
            if employee_id:
                add_comprehensive_employee(employee_id, employee_name, designation, branch_name, shift_name)
                flash(f"Employee data updated: {employee_id}", "success")
            else:
                flash("Employee ID is required.", "error")
        
        elif action == "shift_template":
            # Handle shift template management
            delete_shift_name = request.form.get("delete_shift_name")
            if delete_shift_name:
                delete_shift_template(delete_shift_name)
                flash(f"Shift template deleted: {delete_shift_name}", "success")
                return redirect(url_for("unified_management"))
            
            shift_name = request.form.get("shift_name")
            shift_start = request.form.get("shift_start")
            shift_end = request.form.get("shift_end")
            description = request.form.get("description", "")
            
            if shift_name and shift_start and shift_end:
                add_shift_template(shift_name, shift_start, shift_end, description)
                flash(f"Shift template added: {shift_name}", "success")
            else:
                flash("Shift name, start time, and end time are required.", "error")
        
        elif action == "device_branch":
            # Handle device branch mapping
            delete_serial = request.form.get("delete_serial_number")
            if delete_serial:
                delete_device_branch_mapping(delete_serial)
                flash(f"Device mapping deleted: {delete_serial}", "success")
                return redirect(url_for("unified_management"))
            
            serial_number = request.form.get("serial_number")
            branch_name = request.form.get("branch_name")
            
            if serial_number and branch_name:
                add_device_branch_mapping(serial_number, branch_name)
                flash(f"Device mapping added: {serial_number} → {branch_name}", "success")
            else:
                flash("Serial number and branch name are required.", "error")
        
        elif action == "designation":
            # Handle designation mapping
            employee_id = request.form.get("employee_id")
            designation = request.form.get("designation")
            
            if employee_id and designation:
                add_employee_designation_mapping(employee_id, designation)
                flash(f"Designation mapping added: {employee_id} → {designation}", "success")
            else:
                flash("Employee ID and designation are required.", "error")
        
        elif action == "employee_name":
            # Handle employee name mapping
            employee_id = request.form.get("employee_id")
            employee_name = request.form.get("employee_name")
            
            if employee_id and employee_name:
                add_employee_name_mapping(employee_id, employee_name)
                flash(f"Employee name mapping added: {employee_id} → {employee_name}", "success")
            else:
                flash("Employee ID and employee name are required.", "error")
        
        return redirect(url_for("unified_management"))
    
    # Get data for display
    employees = get_comprehensive_employee_data() or []
    shift_templates = get_shift_templates() or []
    device_mappings = get_device_branch_mappings() or []
    all_branches = sorted({b["branch_name"] for b in device_mappings})
    all_designations = sorted({d["designation"] for d in get_employee_designation_mappings() or []})
    all_employee_names = sorted({n["employee_name"] for n in get_employee_name_mappings() or []})
    
    return render_template("unified_management.html", 
                         employees=employees,
                         shift_templates=shift_templates,
                         device_mappings=device_mappings,
                         all_branches=all_branches,
                         all_designations=all_designations,
                         all_employee_names=all_employee_names)


def ensure_directories_exist() -> None:
    """Ensure the static and templates folders exist."""
    if not os.path.exists("static"):
        os.makedirs("static")
    if not os.path.exists("templates"):
        os.makedirs("templates")


def filter_attendances_by_date(df: pd.DataFrame, start_date: str | None, end_date: str | None) -> pd.DataFrame:
    """Filter attendances by date range."""
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        if start_date:
            df = df[df["timestamp"] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df["timestamp"] <= pd.to_datetime(end_date)]
    return df


def filter_attendances_by_employee(df: pd.DataFrame, employee_id: str) -> pd.DataFrame:
    """Filter attendances by employee ID."""
    return df[df["employee_id"].astype(str).str.contains(str(employee_id), case=False, na=False)]


def filter_attendances_by_branch(df: pd.DataFrame, branch_name: str) -> pd.DataFrame:
    """Filter attendances by branch name."""
    branch_mappings = get_device_branch_mappings() or []
    branch_serials = [b["serial_number"] for b in branch_mappings if branch_name.lower() in b["branch_name"].lower()]
    if branch_serials:
        df = df[df["sn"].isin(branch_serials)]
    return df


def filter_attendances_by_employee_branch(df: pd.DataFrame, employee_branch: str) -> pd.DataFrame:
    """Filter attendances by employee branch."""
    employee_branch_mappings = get_employee_branch_mappings() or []
    branch_employees = [str(eb["employee_id"]) for eb in employee_branch_mappings if employee_branch.lower() in eb["branch_name"].lower()]
    if branch_employees:
        df = df[df["employee_id"].astype(str).isin(branch_employees)]
    return df


def filter_attendances_by_employee_name(df: pd.DataFrame, employee_name: str) -> pd.DataFrame:
    """Filter attendances by employee name."""
    employee_name_mappings = get_employee_name_mappings() or []
    name_employees = [str(en["employee_id"]) for en in employee_name_mappings if employee_name.lower() in en["employee_name"].lower()]
    if name_employees:
        df = df[df["employee_id"].astype(str).isin(name_employees)]
    return df


def filter_attendances_by_designation(df: pd.DataFrame, designation: str) -> pd.DataFrame:
    """Filter attendances by designation."""
    employee_designation_mappings = get_employee_designation_mappings() or []
    designation_employees = [str(ed["employee_id"]) for ed in employee_designation_mappings if designation.lower() in ed["designation"].lower()]
    if designation_employees:
        df = df[df["employee_id"].astype(str).isin(designation_employees)]
    return df


def apply_filters(
    attendences: list[dict[str, Any]],
    start_date: str | None,
    end_date: str | None,
    employee_id: str | None,
    branch_name: str | None,
    employee_branch: str | None,
    employee_name: str | None = None,
    designation: str | None = None
) -> list[dict[str, Any]]:
    """Apply all filters to attendances."""
    if not (start_date or end_date or employee_id or branch_name or employee_branch or employee_name or designation):
        return attendences
    
    df = pd.DataFrame(attendences)
    
    df = filter_attendances_by_date(df, start_date, end_date)
    
    if employee_id:
        df = filter_attendances_by_employee(df, employee_id)
    
    if branch_name:
        df = filter_attendances_by_branch(df, branch_name)
    
    if employee_branch:
        df = filter_attendances_by_employee_branch(df, employee_branch)
    
    if employee_name:
        df = filter_attendances_by_employee_name(df, employee_name)
    
    if designation:
        df = filter_attendances_by_designation(df, designation)
    
    return df.to_dict(orient="records")


def prepare_dashboard_summary(attendences: list[dict[str, Any]], shift_mappings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Prepare summary data for dashboard."""
    summary_df = process_attendance_summary(attendences)
    if summary_df is None or summary_df.empty:
        return []
    
    device_logs = get_device_logs() or []
    finger_logs = get_finger_log() or []
    migration_logs = get_migrations() or []
    user_logs = get_users() or []

    full_summary_df = generate_attendance_summary(attendences, device_logs, finger_logs, migration_logs, user_logs, shift_mappings)
    
    if full_summary_df.empty or "work_status" not in full_summary_df.columns:
        return []
    
    dashboard_summary_df = full_summary_df[(full_summary_df["work_status"] == "worked") & (full_summary_df["day"] != "Subtotal")].copy()
    
    return dashboard_summary_df.to_dict(orient="records")


def add_branch_info_to_summary(summary: list[dict[str, Any]]) -> None:
    """Add branch information to summary records."""
    branch_mappings = get_device_branch_mappings() or []
    branch_map = {str(b["serial_number"]): b["branch_name"] for b in branch_mappings}

    for row in summary:
        row["start_device_sn_branch"] = branch_map.get(str(row.get("start_device_sn")), "")
        row["end_device_sn_branch"] = branch_map.get(str(row.get("end_device_sn")), "")


def add_employee_name_to_summary(summary: list[dict[str, Any]]) -> None:
    """Add employee name information to summary records."""
    name_mappings = get_employee_name_mappings() or []
    name_map = {str(n["employee_id"]): n["employee_name"] for n in name_mappings}

    for row in summary:
        row["employee_name"] = name_map.get(str(row.get("employee_id")), "")


@app.route("/", methods=["GET"])
def index() -> Any:
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")
    employee_id = request.args.get("employee_id")
    branch_name = request.args.get("branch_name")
    employee_branch = request.args.get("employee_branch")
    employee_name = request.args.get("employee_name")
    designation = request.args.get("designation")

    attendences = get_attendences() or []
    attendences = apply_filters(attendences, start_date, end_date, employee_id, branch_name, employee_branch, employee_name, designation)

    shift_mappings = get_user_shift_mappings() or []
    summary = prepare_dashboard_summary(attendences, shift_mappings)
    add_branch_info_to_summary(summary)
    add_employee_name_to_summary(summary)

    summary = sorted(summary, key=lambda x: (x.get("shift_name") or "", x.get("employee_id") or "", x.get("day") or ""))

    all_employees = list({str(a.get("employee_id", "")) for a in get_attendences() or [] if a.get("employee_id")})
    all_branches = list({b["branch_name"] for b in get_device_branch_mappings() or []})
    all_employee_branches = list({eb["branch_name"] for eb in get_employee_branch_mappings() or []})
    
    # Get employee names and designations for searchable dropdowns
    employee_names = get_employee_name_mappings() or []
    employee_designations = get_employee_designation_mappings() or []
    all_employee_names = sorted([name["employee_name"] for name in employee_names if name.get("employee_name")])
    all_designations = sorted(list({des["designation"] for des in employee_designations if des.get("designation")}))

    return render_template(
        "dashboard.html",
        attendences=attendences,
        summary=summary,
        start_date=start_date or "",
        end_date=end_date or "",
        employee_id=employee_id or "",
        branch_name=branch_name or "",
        employee_branch=employee_branch or "",
        employee_name=employee_name or "",
        designation=designation or "",
        all_employees=sorted(all_employees),
        all_branches=sorted(all_branches),
        all_employee_branches=sorted(all_employee_branches),
        all_employee_names=all_employee_names,
        all_designations=all_designations,
        shift_mappings=shift_mappings,
    )


@app.route("/download_xlsx")
def download_xlsx() -> Any:
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")
    employee_id = request.args.get("employee_id")
    branch_name = request.args.get("branch_name")
    employee_branch = request.args.get("employee_branch")
    employee_name = request.args.get("employee_name")
    designation = request.args.get("designation")

    # Validate that both start_date and end_date are provided
    if not start_date or not end_date:
        flash("Both start date and end date are required to download the Excel file.", "error")
        return redirect(url_for("index", start_date=start_date or "", end_date=end_date or "",
                               employee_id=employee_id or "", branch_name=branch_name or "",
                               employee_branch=employee_branch or "", employee_name=employee_name or "",
                               designation=designation or ""))

    attendences = get_attendences() or []
    attendences = apply_filters(attendences, start_date, end_date, employee_id, branch_name, employee_branch, employee_name, designation)

    device_logs = get_device_logs() or []
    finger_logs = get_finger_log() or []
    migration_logs = get_migrations() or []
    user_logs = get_users() or []
    shift_mappings = get_user_shift_mappings() or []
    merged = generate_attendance_summary(attendences, device_logs, finger_logs, migration_logs, user_logs, shift_mappings)
    new_output = write_excel(attendences, device_logs, finger_logs, migration_logs, user_logs, merged)
    
    return send_file(
        new_output,
        as_attachment=True,
        download_name="output.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


if __name__ == "__main__":
    ensure_directories_exist()
    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)
