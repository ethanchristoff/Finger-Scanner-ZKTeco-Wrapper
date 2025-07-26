from flask import Flask, render_template, request, send_file, redirect, url_for, flash
import pandas as pd
from adms_wrapper.core.db_queries import (
    get_attendences,
    get_device_logs,
    get_finger_log,
    get_migrations,
    get_users,
    get_device_branch_mappings,
    add_device_branch_mapping,
    delete_device_branch_mapping,
    get_user_shift_mappings, 
    add_user_shift_mapping, 
    delete_user_shift_mapping
)
from adms_wrapper.__main__ import process_attendance_summary
from adms_wrapper.core.excel_logic import generate_attendance_summary, write_excel

import io
import os

app = Flask(__name__)
app.secret_key = "your_secret_key_here"  # Needed for flash messages

# User Shift Mapping Page
@app.route("/user_shift_mapping", methods=["GET", "POST"])
def user_shift_mapping():
    if request.method == "POST":
        # Handle deletion
        delete_user_id = request.form.get("delete_user_id")
        if delete_user_id:
            delete_user_shift_mapping(delete_user_id)
            flash(f"Shift mapping deleted: {delete_user_id}", "success")
            return redirect(url_for("user_shift_mapping"))
        # Handle addition
        user_id = request.form.get("user_id")
        shift_name = request.form.get("shift_name")
        shift_start = request.form.get("shift_start")
        shift_end = request.form.get("shift_end")
        if user_id and shift_name and shift_start and shift_end:
            add_user_shift_mapping(user_id, shift_name, shift_start, shift_end)
            flash(f"Shift mapping added: {user_id} → {shift_name}", "success")
        else:
            flash("All fields are required.", "error")
        return redirect(url_for("user_shift_mapping"))
    mappings = get_user_shift_mappings() or []
    return render_template("user_shift_mapping.html", mappings=mappings)



@app.route("/device_branch_mapping", methods=["GET", "POST"])
def device_branch_mapping():
    if request.method == "POST":
        # Handle deletion
        delete_sn = request.form.get("delete_serial")
        if delete_sn:
            delete_device_branch_mapping(delete_sn)
            flash(f"Mapping deleted: {delete_sn}", "success")
            return redirect(url_for("device_branch_mapping"))
        # Handle addition
        serial_number = request.form.get("serial_number")
        branch_name = request.form.get("branch_name")
        if serial_number and branch_name:
            add_device_branch_mapping(serial_number, branch_name)
            flash(f"Mapping added: {serial_number} → {branch_name}", "success")
        else:
            flash("Both serial number and branch name are required.", "error")
        return redirect(url_for("device_branch_mapping"))
    mappings = get_device_branch_mappings() or []
    return render_template("device_branch_mapping.html", mappings=mappings)


# Ensure the static and templates folders exist
if not os.path.exists("static"):
    os.makedirs("static")
if not os.path.exists("templates"):
    os.makedirs("templates")


@app.route("/", methods=["GET"])
def index():
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")
    attendences = get_attendences() or []
    # Filter attendences by date if provided
    if start_date or end_date:
        df = pd.DataFrame(attendences)
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            if start_date:
                df = df[df["timestamp"] >= pd.to_datetime(start_date)]
            if end_date:
                df = df[df["timestamp"] <= pd.to_datetime(end_date)]
            attendences = df.to_dict(orient="records")
    shift_mappings = get_user_shift_mappings() or []
    summary_df = process_attendance_summary(attendences)
    summary = summary_df.to_dict(orient="records") if summary_df is not None else []
    # Attach shift info to summary for dashboard sorting and ensure branch names are mapped
    shift_map = {str(s['user_id']): s for s in shift_mappings}
    branch_mappings = get_device_branch_mappings() or []
    branch_map = {str(b['serial_number']): b['branch_name'] for b in branch_mappings}
    for row in summary:
        shift = shift_map.get(str(row.get('employee_id')))
        if shift:
            row['shift_name'] = shift.get('shift_name', '')
            row['shift_start'] = shift.get('shift_start', '')
            row['shift_end'] = shift.get('shift_end', '')
        else:
            row['shift_name'] = ''
            row['shift_start'] = ''
            row['shift_end'] = ''
        # Map start/end device branch names
        row['start_device_sn_branch'] = branch_map.get(str(row.get('start_device_sn')), '')
        row['end_device_sn_branch'] = branch_map.get(str(row.get('end_device_sn')), '')
    # Sort by shift_name if present
    summary = sorted(summary, key=lambda x: (x.get('shift_name') or '', x.get('employee_id') or ''))
    return render_template("dashboard.html", attendences=attendences, summary=summary, start_date=start_date or "", end_date=end_date or "", shift_mappings=shift_mappings)


@app.route("/download_xlsx")
def download_xlsx():
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")
    # Use the same logic as the main function to filter and export
    output = io.BytesIO()
    attendences = get_attendences() or []
    if start_date or end_date:
        df = pd.DataFrame(attendences)
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            if start_date:
                df = df[df["timestamp"] >= pd.to_datetime(start_date)]
            if end_date:
                df = df[df["timestamp"] <= pd.to_datetime(end_date)]
            attendences = df.to_dict(orient="records")
    device_logs = get_device_logs() or []
    finger_logs = get_finger_log() or []
    migration_logs = get_migrations() or []
    user_logs = get_users() or []
    shift_mappings = get_user_shift_mappings() or []
    merged = generate_attendance_summary(attendences, device_logs, finger_logs, migration_logs, user_logs, shift_mappings)
    new_output = write_excel(attendences, device_logs, finger_logs, migration_logs, user_logs, merged)
    return send_file(new_output, as_attachment=True, download_name="output.xlsx", mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)
