#!/usr/bin/env python3

import sqlite3
import os
import sys
import time
import csv

# Paths
HOME = os.path.expanduser("~")

XDG_DATA_HOME = os.environ.get("XDG_DATA_HOME")
if XDG_DATA_HOME:
    BSH_DIR = os.path.join(XDG_DATA_HOME, "bsh")
else:
    BSH_DIR = os.path.join(HOME, ".local", "share", "bsh")

BSH_DB = os.path.join(BSH_DIR, "history.db")

def create_schema(cursor):
    """
    Creates the database schema up to user_version 4,
    matching src/db.cpp exactly.
    """
    current_version_row = cursor.execute("PRAGMA user_version").fetchone()
    current_version = current_version_row[0] if current_version_row else 0

    if current_version == 0:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS commands (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cmd_text TEXT UNIQUE NOT NULL
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS executions (
                id INTEGER PRIMARY KEY,
                command_id INTEGER,
                session_id TEXT,
                cwd TEXT,
                git_branch TEXT,
                exit_code INTEGER,
                duration_ms INTEGER,
                timestamp INTEGER,
                FOREIGN KEY (command_id) REFERENCES commands (id)
            );
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_exec_cwd ON executions(cwd);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_exec_branch ON executions(git_branch);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_exec_ts ON executions(timestamp);")
        cursor.execute("PRAGMA user_version = 1")
        current_version = 1

    if current_version == 1:
        cursor.execute("CREATE VIRTUAL TABLE IF NOT EXISTS commands_fts USING fts5(cmd_text, content='commands', content_rowid='id');")
        cursor.execute("""
            CREATE TRIGGER IF NOT EXISTS commands_ai AFTER INSERT ON commands BEGIN
                INSERT INTO commands_fts(rowid, cmd_text) VALUES (new.id, new.cmd_text);
            END;
        """)
        cursor.execute("INSERT INTO commands_fts(commands_fts) VALUES('rebuild');")
        cursor.execute("PRAGMA user_version = 2")
        current_version = 2

    if current_version == 2:
        # Check if last_timestamp column exists before adding to avoid errors
        try:
            cursor.execute("ALTER TABLE commands ADD COLUMN last_timestamp INTEGER DEFAULT 0;")
        except sqlite3.OperationalError:
            pass

        cursor.execute("""
            UPDATE commands SET last_timestamp = (
                SELECT MAX(timestamp) FROM executions
                WHERE executions.command_id = commands.id
            );
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_cmd_timestamp ON commands(last_timestamp);")
        cursor.execute("PRAGMA user_version = 3")
        current_version = 3

    if current_version == 3:
        cursor.execute("DELETE FROM commands WHERE cmd_text LIKE 'bsh%' OR cmd_text LIKE './bsh%';")
        cursor.execute("INSERT INTO commands_fts(commands_fts) VALUES('rebuild');")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS command_context (
                command_id INTEGER,
                cwd TEXT,
                git_branch TEXT,
                success_count INTEGER DEFAULT 0,
                last_timestamp INTEGER,
                PRIMARY KEY (command_id, cwd, git_branch)
            );
        """)
        cursor.execute("""
            INSERT OR IGNORE INTO command_context (command_id, cwd, git_branch, success_count, last_timestamp)
            SELECT command_id, cwd, COALESCE(git_branch, ''), SUM(CASE WHEN exit_code = 0 THEN 1 ELSE 0 END), MAX(timestamp)
            FROM executions GROUP BY command_id, cwd, COALESCE(git_branch, '')
        """)

        cursor.execute("CREATE INDEX IF NOT EXISTS idx_ctx_cwd ON command_context(cwd);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_ctx_branch ON command_context(git_branch);")

        try:
            cursor.execute("ALTER TABLE commands ADD COLUMN success_count INTEGER DEFAULT 0;")
        except sqlite3.OperationalError:
            pass

        cursor.execute("""
            UPDATE commands SET success_count = (
                SELECT SUM(CASE WHEN exit_code = 0 THEN 1 ELSE 0 END) FROM executions WHERE executions.command_id = commands.id
            );
        """)
        cursor.execute("PRAGMA user_version = 4")
        current_version = 4


def import_csv(csv_path):
    if not os.path.exists(csv_path):
        print(f"Error: File '{csv_path}' does not exist.")
        sys.exit(1)

    # Ensure directory exists
    if not os.path.exists(BSH_DIR):
        os.makedirs(BSH_DIR)

    conn = sqlite3.connect(BSH_DB)
    cursor = conn.cursor()

    # Enable WAL
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA synchronous=NORMAL;")

    create_schema(cursor)

    count = 0
    now = int(time.time())

    try:
        # SQLite python module auto-starts transactions. BEGIN TRANSACTION is only valid if not in autocommit.

        with open(csv_path, 'r', newline='', encoding='utf-8', errors='replace') as f:
            # Try sniffing the file to check for headers
            sample = f.read(1024)
            f.seek(0)

            # Simple fallback check for headers
            has_header = False
            first_line = sample.split('\n')[0].lower()
            if first_line.startswith('command'):
                has_header = True

            reader = csv.reader(f)

            if has_header:
                next(reader, None) # Skip header

            for row in reader:
                if not row:
                    continue

                cmd = row[0].strip()
                if not cmd:
                    continue

                cwd = row[1].strip() if len(row) > 1 else ""

                # 1. Insert Command (Ignore duplicates)
                cursor.execute("INSERT OR IGNORE INTO commands (cmd_text) VALUES (?)", (cmd,))

                # 2. Get the ID
                cursor.execute("SELECT id FROM commands WHERE cmd_text = ?", (cmd,))
                result = cursor.fetchone()

                if result:
                    cmd_id = result[0]
                    # 3. Insert Execution with spoofed telemetry
                    cursor.execute("""
                        INSERT INTO executions
                        (command_id, session_id, cwd, git_branch, exit_code, duration_ms, timestamp)
                        VALUES (?, 'preloaded_csv', ?, '', 0, 0, ?)
                    """, (cmd_id, cwd, now))

                    # 4. Upsert fast-path context table
                    cursor.execute("""
                        INSERT INTO command_context (command_id, cwd, git_branch, success_count, last_timestamp)
                        VALUES (?, ?, '', 1, ?)
                        ON CONFLICT(command_id, cwd, git_branch) DO UPDATE SET
                        success_count = success_count + 1,
                        last_timestamp = MAX(last_timestamp, excluded.last_timestamp)
                    """, (cmd_id, cwd, now))

                    # 5. Update fast-path global table
                    cursor.execute("""
                        UPDATE commands SET last_timestamp = ?, success_count = success_count + 1 WHERE id = ?
                    """, (now, cmd_id))

                    count += 1

        conn.commit()
        print(f"Success! Imported {count} commands cleanly.")

    except Exception as e:
        print(f"Error during import: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 import_csv.py <path_to_csv>")
        sys.exit(1)

    csv_file = sys.argv[1]
    import_csv(csv_file)
