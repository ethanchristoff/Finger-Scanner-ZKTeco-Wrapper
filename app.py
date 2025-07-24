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
)
from adms_wrapper.__main__ import process_attendance_summary
from adms_wrapper.core.excel_logic import generate_attendance_summary, write_excel

import io
import os

app = Flask(__name__)
app.secret_key = "your_secret_key_here"  # Needed for flash messages


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
    summary_df = process_attendance_summary(attendences)
    summary = summary_df.to_dict(orient="records") if summary_df is not None else []
    return render_template("dashboard.html", attendences=attendences, summary=summary, start_date=start_date or "", end_date=end_date or "")


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
    merged = generate_attendance_summary(attendences, device_logs, finger_logs, migration_logs, user_logs)
    new_output = write_excel(attendences, device_logs, finger_logs, migration_logs, user_logs, merged)
    return send_file(new_output, as_attachment=True, download_name="output.xlsx", mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)
