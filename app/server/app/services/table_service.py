# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Table discovery service."""
from typing import Optional
from app.database import get_conn, _fetchall, _fetchone
from app.models import TableInfo

def _fix_row(r: dict) -> dict:
    """Convert DB int to bool for partitioned field."""
    r["partitioned"] = bool(r.get("partitioned", 0))
    return r

def list_tables(database: Optional[str] = None, format: Optional[str] = None) -> list[TableInfo]:
    conn = get_conn()
    sql = "SELECT database_name as database, table_name, format, location, aws_account, size_bytes, partitioned, owner FROM tables_inventory WHERE 1=1"
    params: list = []
    if database:
        sql += " AND database_name = ?"
        params.append(database)
    if format:
        sql += " AND format = ?"
        params.append(format)
    rows = _fetchall(conn, sql, tuple(params) if params else None)
    conn.close()
    return [TableInfo(**_fix_row(r)) for r in rows]

def get_table(database: str, table_name: str) -> Optional[TableInfo]:
    conn = get_conn()
    row = _fetchone(conn, "SELECT database_name as database, table_name, format, location, aws_account, size_bytes, partitioned, owner FROM tables_inventory WHERE database_name=? AND table_name=?", (database, table_name))
    conn.close()
    return TableInfo(**_fix_row(row)) if row else None

def update_owner(tables: list[dict], owner: str) -> int:
    """Update the owner field for a list of tables."""
    from app.database import _run_sql, _commit
    conn = get_conn()
    count = 0
    for t in tables:
        cur = _run_sql(conn,
            "UPDATE tables_inventory SET owner=? WHERE database_name=? AND table_name=?",
            (owner, t["database"], t["table_name"]))
        count += cur.rowcount
    _commit(conn)
    conn.close()
    return count


def delete_tables(tables: list[dict]) -> int:
    """Delete a list of tables from the inventory."""
    from app.database import _run_sql, _commit
    conn = get_conn()
    count = 0
    for t in tables:
        cur = _run_sql(conn,
            "DELETE FROM tables_inventory WHERE database_name=? AND table_name=?",
            (t["database"], t["table_name"]))
        count += cur.rowcount
    _commit(conn)
    conn.close()
    return count
