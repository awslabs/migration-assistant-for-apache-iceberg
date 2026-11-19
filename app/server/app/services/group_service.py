# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Group management service."""
from app.database import get_conn, _fetchall, _fetchone, _run_sql, _commit
from app.models import GroupInfo, CreateGroupRequest, AddGroupMemberRequest

def list_groups() -> list[GroupInfo]:
    conn = get_conn()
    groups = _fetchall(conn, "SELECT * FROM groups ORDER BY name")
    result = []
    for g in groups:
        members = _fetchall(conn, "SELECT gm.username, gm.role, u.name FROM group_members gm JOIN users u ON gm.username = u.username WHERE gm.group_name=?", (g["name"],))
        result.append(GroupInfo(name=g["name"], description=g["description"], members=members))
    conn.close()
    return result

def create_group(request: CreateGroupRequest) -> GroupInfo:
    conn = get_conn()
    try:
        _run_sql(conn, "INSERT INTO groups VALUES (?,?)", (request.name, request.description))
        _commit(conn)
    except Exception:
        conn.close()
        raise ValueError(f"Group '{request.name}' already exists")
    conn.close()
    return GroupInfo(name=request.name, description=request.description)

def add_member(group_name: str, request: AddGroupMemberRequest):
    conn = get_conn()
    role_val = request.role.value if hasattr(request.role, "value") else request.role
    try:
        _run_sql(conn, "INSERT INTO group_members (group_name,username,role) VALUES (?,?,?)", (group_name, request.username, role_val))
        _commit(conn)
    except Exception:
        conn.close()
        raise ValueError("Member already in group or group/user not found")
    conn.close()
    return list_groups_single(group_name)

def remove_member(group_name: str, username: str):
    conn = get_conn()
    _run_sql(conn, "DELETE FROM group_members WHERE group_name=? AND username=?", (group_name, username))
    _commit(conn)
    conn.close()
    return list_groups_single(group_name)

def list_groups_single(name: str):
    conn = get_conn()
    g = _fetchone(conn, "SELECT * FROM groups WHERE name=?", (name,))
    if not g:
        conn.close()
        return None
    members = _fetchall(conn, "SELECT gm.username, gm.role, u.name FROM group_members gm JOIN users u ON gm.username = u.username WHERE gm.group_name=?", (name,))
    conn.close()
    return GroupInfo(name=g["name"], description=g["description"], members=members)

def delete_group(name: str) -> bool:
    if name == "admins":
        raise ValueError("The 'admins' group cannot be deleted")
    conn = get_conn()
    _run_sql(conn, "DELETE FROM group_members WHERE group_name=?", (name,))
    cur = _run_sql(conn, "DELETE FROM groups WHERE name=?", (name,))
    _commit(conn)
    deleted = cur.rowcount > 0
    conn.close()
    return deleted
