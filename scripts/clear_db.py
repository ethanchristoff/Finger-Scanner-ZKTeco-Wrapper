#!/usr/bin/env python3
"""Clear application data from the MySQL database.

This script truncates the application's tables (mappings, users, logs, attendances, etc.).
It is intentional destructive — make a backup before running.

Usage:
  python scripts/clear_db.py --backup    # creates a mysqldump backup then asks to proceed
  python scripts/clear_db.py --yes       # run non-interactively (no prompt)
  python scripts/clear_db.py --dry-run   # show what would be done

The script reads DB connection settings from the project's `adms_wrapper.core.db_connector.DB_CONFIG`.
If you use --backup the script will try to run `mysqldump` on PATH; if it's unavailable it will skip backup.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# Ensure project root is on sys.path so `adms_wrapper` can be imported when running this script directly
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from adms_wrapper.core.db_connector import DB_CONFIG, get_connection

TABLES_TO_CLEAR = [
    # Core records
    "attendances",
    "device_log",
    "finger_log",
    "migrations",
    "users",
    # Mappings and auxiliary tables
    "branch_mapping",
    "employee_branch_mapping",
    "employee_designation_mapping",
    "employee_name_mapping",
    "user_shift_mapping",
    "shift_template",
    "settings",
]


def run_backup(backup_dir: Path) -> Path | None:
    """Run mysqldump and store backup in backup_dir. Returns path or None if it failed."""
    dump_path = backup_dir / f"db_backup_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.sql"

    # Build mysqldump command from DB_CONFIG
    # NOTE: Passing password on command line is not ideal; this is a convenience for local use.
    user = DB_CONFIG.get("user")
    host = DB_CONFIG.get("host")
    port = DB_CONFIG.get("port")
    database = DB_CONFIG.get("database")
    password = DB_CONFIG.get("password")

    mysqldump = shutil.which("mysqldump")
    if not mysqldump:
        print("mysqldump not found in PATH; skipping backup.")
        return None

    cmd = [mysqldump, f"-h{host}", f"-P{port}", f"-u{user}", f"-p{password}", database]
    # Replace last arg with actual database name variable
    cmd[-1] = database

    print(f"Running backup (mysqldump) -> {dump_path} ...")
    try:
        with open(dump_path, "wb") as f:
            proc = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE)
        if proc.returncode != 0:
            print("mysqldump failed:", proc.stderr.decode(errors="ignore"))
            return None
        print("Backup created:", dump_path)
        return dump_path
    except Exception as e:
        print("Backup failed:", e)
        return None


def clear_tables(confirm: bool, dry_run: bool) -> dict[str, str]:
    """Clear listed tables. Returns a dict of table->status."""
    results: dict[str, str] = {}

    if dry_run:
        print("Dry run: the following tables would be cleared:")
        for t in TABLES_TO_CLEAR:
            print(" -", t)
        return {t: "dry-run" for t in TABLES_TO_CLEAR}

    conn = get_connection()
    if not conn:
        raise RuntimeError("Could not connect to the database; aborting")

    try:
        cursor = conn.cursor()
        # Disable foreign key checks to allow truncation order independence
        cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
        conn.commit()

        for table in TABLES_TO_CLEAR:
            try:
                sql = f"TRUNCATE TABLE {table};"
                cursor.execute(sql)
                conn.commit()
                results[table] = "cleared"
            except Exception as e:
                # If TRUNCATE fails (permissions/constraints), fall back to DELETE
                try:
                    cursor.execute(f"DELETE FROM {table};")
                    conn.commit()
                    results[table] = "deleted-via-delete"
                except Exception as e2:
                    results[table] = f"failed: {e2}"
        # Re-enable foreign key checks
        cursor.execute("SET FOREIGN_KEY_CHECKS=1;")
        conn.commit()
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

    return results


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Clear application database tables (destructive).")
    p.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")
    p.add_argument("--dry-run", action="store_true", help="Show what would be done without modifying the database")
    p.add_argument("--backup", action="store_true", help="Attempt to create a mysqldump backup before clearing")
    p.add_argument("--backup-dir", default="backups", help="Directory where backups are stored")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    print("This will remove data from the following tables:")
    for t in TABLES_TO_CLEAR:
        print(" -", t)

    if args.dry_run:
        print("\nPerforming dry-run. No changes will be made.")

    if not args.yes and not args.dry_run:
        confirm = input("Are you sure you want to proceed? Type 'YES' to continue: ")
        if confirm.strip() != "YES":
            print("Aborted by user.")
            return 1

    backup_path = None
    if args.backup:
        backup_dir = Path(args.backup_dir)
        backup_dir.mkdir(parents=True, exist_ok=True)
        backup_path = run_backup(backup_dir)
        if not backup_path:
            print("Backup failed or skipped. Aborting to avoid data loss.")
            return 2

    try:
        results = clear_tables(confirm=args.yes, dry_run=args.dry_run)
    except Exception as e:
        print("Error while clearing tables:", e)
        return 3

    print("\nOperation results:")
    for table, status in results.items():
        print(f" - {table}: {status}")

    if backup_path:
        print("Backup saved at:", backup_path)

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
