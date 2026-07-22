#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sqlite3
import sys
import tempfile
import unittest


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
TOOLS_DIR = os.path.join(os.path.dirname(THIS_DIR), "tools")
if TOOLS_DIR not in sys.path:
    sys.path.insert(0, TOOLS_DIR)

from check_revision_db_health import (  # noqa: E402
    REQUIRED_OPS_SCHEMA,
    REQUIRED_PLAN_SCHEMA,
    _connect,
    _fetch_rows,
    build_report,
)


def _create_schema_db(path, schema):
    conn = sqlite3.connect(path)
    try:
        for table_name, columns in sorted(schema.items()):
            definitions = ", ".join('"%s" TEXT' % column for column in sorted(columns))
            conn.execute('CREATE TABLE "%s" (%s);' % (table_name, definitions))
        conn.commit()
    finally:
        conn.close()


class RevisionDbHealthFailClosedTest(unittest.TestCase):
    def test_valid_empty_schema_is_read_only_and_reportable(self):
        with tempfile.TemporaryDirectory() as root:
            plan_path = os.path.join(root, "planning.db")
            ops_path = os.path.join(root, "operations.db")
            _create_schema_db(plan_path, REQUIRED_PLAN_SCHEMA)
            _create_schema_db(ops_path, REQUIRED_OPS_SCHEMA)

            report = build_report(
                plan_db_path=plan_path,
                ops_db_path=ops_path,
                robot_id="CR-001",
            )

            self.assertTrue(report["summary"]["ok"])
            conn = _connect(plan_path)
            try:
                with self.assertRaises(sqlite3.OperationalError):
                    conn.execute("CREATE TABLE forbidden_write(value TEXT);")
            finally:
                conn.close()

    def test_missing_required_table_fails_closed(self):
        with tempfile.TemporaryDirectory() as root:
            plan_path = os.path.join(root, "planning.db")
            conn = sqlite3.connect(plan_path)
            conn.execute("CREATE TABLE unrelated(value TEXT);")
            conn.commit()
            conn.close()

            with self.assertRaisesRegex(RuntimeError, "missing required tables"):
                build_report(plan_db_path=plan_path, robot_id="CR-001")

    def test_missing_required_column_fails_closed(self):
        with tempfile.TemporaryDirectory() as root:
            plan_path = os.path.join(root, "planning.db")
            broken_schema = dict(REQUIRED_PLAN_SCHEMA)
            broken_schema["map_assets"] = {"map_name"}
            _create_schema_db(plan_path, broken_schema)

            with self.assertRaisesRegex(RuntimeError, "missing required columns"):
                build_report(plan_db_path=plan_path, robot_id="CR-001")

    def test_corrupt_database_fails_closed(self):
        with tempfile.TemporaryDirectory() as root:
            plan_path = os.path.join(root, "planning.db")
            with open(plan_path, "wb") as handle:
                handle.write(b"not a sqlite database")

            with self.assertRaises(sqlite3.DatabaseError):
                build_report(plan_db_path=plan_path, robot_id="CR-001")

    def test_select_errors_are_not_silently_converted_to_empty_rows(self):
        with tempfile.TemporaryDirectory() as root:
            plan_path = os.path.join(root, "planning.db")
            _create_schema_db(plan_path, REQUIRED_PLAN_SCHEMA)
            conn = _connect(plan_path)
            try:
                with self.assertRaises(sqlite3.OperationalError):
                    _fetch_rows(conn, "SELECT * FROM table_that_does_not_exist;")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
