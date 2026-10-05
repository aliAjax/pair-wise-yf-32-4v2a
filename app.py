#!/usr/bin/env python3
"""Organ allocation and cold-chain coordination service (standard library only)."""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import date, datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

PORT = 8203
ROLES = {"viewer", "hospital", "coordinator", "allocation_officer", "auditor"}
STATUSES = {"proposed", "accepted", "in_transit", "handed_off", "implanted", "withdrawn", "expired"}
HOLD_STATUSES = {"held", "queued", "released", "consumed"}
OCCUPYING = ("held", "consumed")  # consumes a surgery slot for the day


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, details: dict[str, Any] | None = None):
        super().__init__(message); self.status, self.code, self.message, self.details = status, code, message, details or {}


def utcnow() -> datetime: return datetime.now(timezone.utc)
def iso(value: datetime | None = None) -> str: return (value or utcnow()).replace(microsecond=0).isoformat().replace("+00:00", "Z")
def parse_time(value: str | None) -> datetime:
    if not value: raise ApiError(400, "time_required", "必须提供 ISO 8601 时间")
    try: parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc: raise ApiError(400, "invalid_time", f"时间格式错误: {value}") from exc
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def parse_slot_date(value: Any) -> date:
    try: return date.fromisoformat(str(value))
    except (TypeError, ValueError) as exc: raise ApiError(400, "invalid_slot_date", "slot_date 必须为 YYYY-MM-DD") from exc


def blood_compatible(donor: str, recipient: str) -> bool:
    return {
        "O": {"O", "A", "B", "AB"}, "A": {"A", "AB"}, "B": {"B", "AB"}, "AB": {"AB"},
    }.get(donor.upper(), set()) and recipient.upper() in {"O", "A", "B", "AB"} and recipient.upper() in {
        "O": {"O", "A", "B", "AB"}, "A": {"A", "AB"}, "B": {"B", "AB"}, "AB": {"AB"},
    }.get(donor.upper(), set())


class Repository:
    """SQLite storage with one connection per worker thread so BEGIN IMMEDIATE serializes writers."""

    SCHEMA = """
    CREATE TABLE IF NOT EXISTS donors(
        id INTEGER PRIMARY KEY AUTOINCREMENT, blood_type TEXT NOT NULL, organ TEXT NOT NULL, hospital TEXT NOT NULL,
        region TEXT NOT NULL, available_at TEXT NOT NULL, expires_at TEXT NOT NULL, clinical_match INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'available', revision INTEGER NOT NULL DEFAULT 1, created_by TEXT NOT NULL, created_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS candidates(
        id INTEGER PRIMARY KEY AUTOINCREMENT, patient_name TEXT NOT NULL, blood_type TEXT NOT NULL, organ TEXT NOT NULL,
        hospital TEXT NOT NULL, region TEXT NOT NULL, urgency INTEGER NOT NULL, wait_days INTEGER NOT NULL,
        willing INTEGER NOT NULL DEFAULT 1, clinical_match INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'active',
        created_by TEXT NOT NULL, created_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS surgery_slots(
        id INTEGER PRIMARY KEY AUTOINCREMENT, hospital TEXT NOT NULL, slot_date TEXT NOT NULL,
        room_count INTEGER NOT NULL, anesthesia_team_count INTEGER,
        created_by TEXT NOT NULL, created_at TEXT NOT NULL, UNIQUE(hospital, slot_date)
    );
    CREATE TABLE IF NOT EXISTS slot_holds(
        id INTEGER PRIMARY KEY AUTOINCREMENT, slot_id INTEGER NOT NULL REFERENCES surgery_slots(id),
        allocation_id INTEGER UNIQUE REFERENCES allocations(id), client_token TEXT,
        status TEXT NOT NULL DEFAULT 'held', enqueued_at TEXT, held_at TEXT, released_at TEXT, created_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS allocations(
        id INTEGER PRIMARY KEY AUTOINCREMENT, donor_id INTEGER NOT NULL UNIQUE REFERENCES donors(id), candidate_id INTEGER NOT NULL REFERENCES candidates(id),
        score REAL NOT NULL, status TEXT NOT NULL DEFAULT 'proposed', revision INTEGER NOT NULL DEFAULT 1,
        cold_chain_temp REAL, delayed_minutes INTEGER NOT NULL DEFAULT 0, created_by TEXT NOT NULL, created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL, accepted_at TEXT, implanted_at TEXT, hold_id INTEGER, client_token TEXT
    );
    CREATE TABLE IF NOT EXISTS handoffs(
        id INTEGER PRIMARY KEY AUTOINCREMENT, allocation_id INTEGER NOT NULL REFERENCES allocations(id), from_hospital TEXT NOT NULL,
        to_hospital TEXT NOT NULL, cold_chain_temp REAL NOT NULL, status TEXT NOT NULL DEFAULT 'initiated',
        initiated_by TEXT NOT NULL, accepted_by TEXT, initiated_at TEXT NOT NULL, accepted_at TEXT,
        UNIQUE(allocation_id)
    );
    CREATE TABLE IF NOT EXISTS audit_log(
        id INTEGER PRIMARY KEY AUTOINCREMENT, allocation_id INTEGER, donor_id INTEGER, actor TEXT NOT NULL, role TEXT NOT NULL,
        action TEXT NOT NULL, detail_json TEXT NOT NULL, created_at TEXT NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_holds_client_token ON slot_holds(client_token) WHERE client_token IS NOT NULL;
    CREATE UNIQUE INDEX IF NOT EXISTS idx_allocations_client_token ON allocations(client_token) WHERE client_token IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_holds_slot ON slot_holds(slot_id, status);
    """

    def __init__(self, path: str | Path):
        self.path = str(path)
        self._local = threading.local()
        bootstrap = self._connect()
        try:
            bootstrap.executescript(self.SCHEMA)
            columns = {r["name"] for r in bootstrap.execute("PRAGMA table_info(allocations)")}
            if "hold_id" not in columns:
                bootstrap.execute("ALTER TABLE allocations ADD COLUMN hold_id INTEGER")
            if "client_token" not in columns:
                bootstrap.execute("ALTER TABLE allocations ADD COLUMN client_token TEXT")
            bootstrap.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_allocations_client_token ON allocations(client_token) WHERE client_token IS NOT NULL")
        finally:
            bootstrap.close()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, isolation_level=None, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    @property
    def conn(self) -> sqlite3.Connection:
        current = getattr(self._local, "conn", None)
        if current is None:
            current = self._connect(); self._local.conn = current
        return current

    @contextmanager
    def tx(self):
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield self.conn; self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK"); raise

    @staticmethod
    def audit(conn: sqlite3.Connection, allocation_id: int | None, donor_id: int | None, actor: str, role: str, action: str, detail: dict[str, Any]) -> None:
        conn.execute("INSERT INTO audit_log(allocation_id,donor_id,actor,role,action,detail_json,created_at) VALUES(?,?,?,?,?,?,?)",
                     (allocation_id, donor_id, actor, role, action, json.dumps(detail, ensure_ascii=False, sort_keys=True), iso()))


class OrganAllocationService:
    def __init__(self, path: str | Path):
        self.repo = Repository(path)
        # Test seam: the next allocation insert raises a persistence fault after the hold is already committed.
        self.fault_on_next_allocation_insert = False

    @staticmethod
    def identity(headers: Any) -> tuple[str, str, str]:
        actor, role, hospital = headers.get("X-User-Id", "").strip(), headers.get("X-Role", "").strip(), headers.get("X-Hospital", "").strip()
        if not actor or role not in ROLES: raise ApiError(401, "unauthorized", "需要 X-User-Id 和有效 X-Role")
        if role == "hospital" and not hospital: raise ApiError(401, "hospital_required", "医院角色必须提供 X-Hospital")
        return actor, role, hospital

    @staticmethod
    def _row(row: sqlite3.Row | None) -> dict[str, Any] | None: return dict(row) if row else None

    @staticmethod
    def _capacity(slot: sqlite3.Row) -> int:
        rooms = int(slot["room_count"])
        teams = slot["anesthesia_team_count"]
        return rooms if teams is None else min(rooms, int(teams))

    def register_donor(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"coordinator", "allocation_officer"}: raise ApiError(403, "donor_forbidden", "当前角色不能登记器官")
        required = ("blood_type", "organ", "hospital", "region", "available_at", "expires_at")
        if any(not body.get(k) for k in required): raise ApiError(400, "missing_fields", f"缺少字段: {', '.join(k for k in required if not body.get(k))}")
        blood, organ = str(body["blood_type"]).upper(), str(body["organ"]).lower()
        if blood not in {"O", "A", "B", "AB"}: raise ApiError(400, "invalid_blood_type", "血型必须为 O/A/B/AB")
        available, expires = parse_time(body["available_at"]), parse_time(body["expires_at"])
        if expires <= available: raise ApiError(400, "invalid_window", "可用窗口结束时间必须晚于开始时间")
        with self.repo.tx() as conn:
            cur = conn.execute("""INSERT INTO donors(blood_type,organ,hospital,region,available_at,expires_at,clinical_match,created_by,created_at)
                                  VALUES(?,?,?,?,?,?,?,?,?)""",
                               (blood, organ, body["hospital"], body["region"], iso(available), iso(expires), int(body.get("clinical_match", 0)), actor, iso()))
            donor_id = cur.lastrowid; Repository.audit(conn, None, donor_id, actor, role, "donor_registered", {"organ": organ, "expires_at": iso(expires)})
            return dict(conn.execute("SELECT * FROM donors WHERE id=?", (donor_id,)).fetchone())

    def register_candidate(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"coordinator", "allocation_officer"}: raise ApiError(403, "candidate_forbidden", "当前角色不能登记候选患者")
        required = ("patient_name", "blood_type", "organ", "hospital", "region")
        if any(not body.get(k) for k in required): raise ApiError(400, "missing_fields", "候选患者基础信息不完整")
        blood = str(body["blood_type"]).upper(); urgency = body.get("urgency"); wait_days = body.get("wait_days", 0)
        if blood not in {"O", "A", "B", "AB"} or not isinstance(urgency, int) or not 1 <= urgency <= 5 or not isinstance(wait_days, int) or wait_days < 0:
            raise ApiError(400, "invalid_candidate", "血型、1-5 紧急度和非负等待天数无效")
        with self.repo.tx() as conn:
            cur = conn.execute("""INSERT INTO candidates(patient_name,blood_type,organ,hospital,region,urgency,wait_days,willing,clinical_match,created_by,created_at)
                                  VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                               (body["patient_name"], blood, str(body["organ"]).lower(), body["hospital"], body["region"], urgency, wait_days,
                                int(body.get("willing", True)), int(body.get("clinical_match", 0)), actor, iso()))
            return dict(conn.execute("SELECT * FROM candidates WHERE id=?", (cur.lastrowid,)).fetchone())

    # ----- surgery slot capacity -------------------------------------------------

    def register_slot(self, actor: str, role: str, hospital: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "hospital": raise ApiError(403, "slot_forbidden", "只有医院可以登记手术间接台容量")
        day = parse_slot_date(body.get("slot_date"))
        rooms = body.get("room_count")
        teams = body.get("anesthesia_team_count", None)
        if not isinstance(rooms, int) or not 1 <= rooms <= 50:
            raise ApiError(400, "invalid_room_count", "room_count 必须为 1-50 的整数")
        if teams is not None and (not isinstance(teams, int) or not 1 <= teams <= 50):
            raise ApiError(400, "invalid_anesthesia_team_count", "anesthesia_team_count 必须为 1-50 的整数或省略")
        day_text = day.isoformat()
        with self.repo.tx() as conn:
            slot = conn.execute("SELECT * FROM surgery_slots WHERE hospital=? AND slot_date=?", (hospital, day_text)).fetchone()
            new_capacity = rooms if teams is None else min(rooms, teams)
            if slot:
                occupied = conn.execute("SELECT COUNT(*) FROM slot_holds WHERE slot_id=? AND status IN (?,?)", (slot["id"], *OCCUPYING)).fetchone()[0]
                if new_capacity < occupied:
                    raise ApiError(409, "capacity_below_held", f"当前已占用 {occupied} 间，容量不能调减到 {new_capacity}",
                                  {"occupied": occupied, "capacity": new_capacity})
                conn.execute("UPDATE surgery_slots SET room_count=?,anesthesia_team_count=? WHERE id=?", (rooms, teams, slot["id"]))
                Repository.audit(conn, None, None, actor, role, "slot_updated",
                                 {"hospital": hospital, "slot_date": day_text, "room_count": rooms, "anesthesia_team_count": teams})
                slot = conn.execute("SELECT * FROM surgery_slots WHERE id=?", (slot["id"],)).fetchone()
            else:
                cur = conn.execute("""INSERT INTO surgery_slots(hospital,slot_date,room_count,anesthesia_team_count,created_by,created_at)
                                      VALUES(?,?,?,?,?,?)""", (hospital, day_text, rooms, teams, actor, iso()))
                slot = conn.execute("SELECT * FROM surgery_slots WHERE id=?", (cur.lastrowid,)).fetchone()
                Repository.audit(conn, None, None, actor, role, "slot_registered",
                                 {"hospital": hospital, "slot_date": day_text, "room_count": rooms, "anesthesia_team_count": teams})
            self._promote_queue(conn, slot, actor, role)
            return self._slot_view(conn, slot)

    def _occupied(self, conn: sqlite3.Connection, slot_id: int) -> int:
        return conn.execute("SELECT COUNT(*) FROM slot_holds WHERE slot_id=? AND status IN (?,?)", (slot_id, *OCCUPYING)).fetchone()[0]

    def _queue_position(self, conn: sqlite3.Connection, hold_id: int) -> int:
        return 1 + conn.execute("SELECT COUNT(*) FROM slot_holds WHERE slot_id=(SELECT slot_id FROM slot_holds WHERE id=?) AND status='queued' AND id<?",
                                (hold_id, hold_id)).fetchone()[0]

    def _hold_summary(self, conn: sqlite3.Connection, hold: sqlite3.Row) -> dict[str, Any]:
        slot = conn.execute("SELECT * FROM surgery_slots WHERE id=?", (hold["slot_id"],)).fetchone()
        occupied = self._occupied(conn, hold["slot_id"])
        summary = {
            "hold_id": hold["id"], "status": hold["status"], "hospital": slot["hospital"], "slot_date": slot["slot_date"],
            "room_count": slot["room_count"], "anesthesia_team_count": slot["anesthesia_team_count"],
            "capacity": self._capacity(slot), "occupied": occupied, "remaining": max(0, self._capacity(slot) - occupied),
        }
        if hold["status"] == "queued":
            summary["queue_position"] = self._queue_position(conn, hold["id"])
        return summary

    def _slot_view(self, conn: sqlite3.Connection, slot: sqlite3.Row) -> dict[str, Any]:
        capacity = self._capacity(slot)
        occupied = self._occupied(conn, slot["id"])
        queue = []
        for position, hold in enumerate(conn.execute("SELECT * FROM slot_holds WHERE slot_id=? AND status='queued' ORDER BY id", (slot["id"],)), 1):
            item = {"position": position, "hold_id": hold["id"], "allocation_id": hold["allocation_id"], "enqueued_at": hold["enqueued_at"]}
            if hold["allocation_id"]:
                link = conn.execute("""SELECT a.id,a.donor_id,a.candidate_id,d.organ,c.patient_name
                                       FROM allocations a JOIN donors d ON d.id=a.donor_id JOIN candidates c ON c.id=a.candidate_id
                                       WHERE a.id=?""", (hold["allocation_id"],)).fetchone()
                if link: item.update({"donor_id": link["donor_id"], "candidate_id": link["candidate_id"], "organ": link["organ"], "patient_name": link["patient_name"]})
            queue.append(item)
        return {
            "slot_id": slot["id"], "hospital": slot["hospital"], "slot_date": slot["slot_date"],
            "room_count": slot["room_count"], "anesthesia_team_count": slot["anesthesia_team_count"],
            "capacity": capacity, "occupied": occupied, "remaining": max(0, capacity - occupied),
            "queued_count": len(queue), "queue": queue,
        }

    def _slots_data(self, conn: sqlite3.Connection, role: str, hospital: str) -> list[dict[str, Any]]:
        if role == "hospital":
            rows = conn.execute("SELECT * FROM surgery_slots WHERE hospital=? ORDER BY slot_date", (hospital,))
        elif role in {"coordinator", "allocation_officer", "auditor"}:
            rows = conn.execute("SELECT * FROM surgery_slots ORDER BY slot_date,hospital")
        else:
            return []
        return [self._slot_view(conn, row) for row in rows]

    def slots(self, role: str, hospital: str) -> dict[str, Any]:
        if role == "viewer": raise ApiError(403, "slot_forbidden", "当前角色不能查看手术间排期")
        with self.repo.tx() as conn:
            return {"slots": self._slots_data(conn, role, hospital), "server_time": iso()}

    def _expire_allocation(self, conn: sqlite3.Connection, allocation: sqlite3.Row, donor: sqlite3.Row, actor: str, role: str) -> None:
        conn.execute("UPDATE allocations SET status='expired',revision=revision+1,updated_at=? WHERE id=?", (iso(), allocation["id"]))
        conn.execute("UPDATE donors SET status='expired',revision=revision+1 WHERE id=?", (donor["id"],))
        Repository.audit(conn, allocation["id"], donor["id"], actor, role, "allocation_expired", {"reason": "organ_window_elapsed"})

    def _release_hold(self, conn: sqlite3.Connection, hold_id: int, reason: str, actor: str, role: str) -> None:
        hold = conn.execute("SELECT * FROM slot_holds WHERE id=?", (hold_id,)).fetchone()
        if not hold or hold["status"] == "released": return
        conn.execute("UPDATE slot_holds SET status='released',released_at=? WHERE id=?", (iso(), hold_id))
        detail = {"reason": reason, "slot_id": hold["slot_id"]}
        if hold["allocation_id"]:
            row = conn.execute("SELECT donor_id FROM allocations WHERE id=?", (hold["allocation_id"],)).fetchone()
            donor_id = row["donor_id"] if row else None
        else:
            donor_id = None
        Repository.audit(conn, hold["allocation_id"], donor_id, actor, role, "slot_released", detail)

    def _sweep_expired_holds(self, conn: sqlite3.Connection, slot: sqlite3.Row, actor: str, role: str) -> None:
        """Release holds whose organ window has elapsed (lazy expiry for allocations nobody has touched)."""
        now = utcnow()
        rows = conn.execute("""SELECT h.id hold_id,a.*,d.expires_at donor_expires FROM slot_holds h
                               JOIN allocations a ON a.id=h.allocation_id JOIN donors d ON d.id=a.donor_id
                               WHERE h.slot_id=? AND h.status IN ('held','queued')""", (slot["id"],)).fetchall()
        for row in rows:
            if parse_time(row["donor_expires"]) <= now:
                donor = conn.execute("SELECT * FROM donors WHERE id=?", (row["donor_id"],)).fetchone()
                allocation = conn.execute("SELECT * FROM allocations WHERE id=?", (row["allocation_id"],)).fetchone()
                if allocation and donor and allocation["status"] not in {"withdrawn", "expired", "implanted"}:
                    self._expire_allocation(conn, allocation, donor, actor, role)
                self._release_hold(conn, row["hold_id"], "organ_expired", actor, role)

    def _promote_queue(self, conn: sqlite3.Connection, slot: sqlite3.Row, actor: str, role: str) -> None:
        """Move the oldest queued holds into free slots, FIFO. Expired heads are released and skipped."""
        while True:
            capacity = self._capacity(slot)
            if self._occupied(conn, slot["id"]) >= capacity: return
            head = conn.execute("SELECT * FROM slot_holds WHERE slot_id=? AND status='queued' ORDER BY id LIMIT 1", (slot["id"],)).fetchone()
            if not head: return
            if head["allocation_id"]:
                linked = conn.execute("""SELECT a.*,d.expires_at donor_expires FROM allocations a JOIN donors d ON d.id=a.donor_id
                                         WHERE a.id=?""", (head["allocation_id"],)).fetchone()
                if linked and parse_time(linked["donor_expires"]) <= utcnow():
                    donor = conn.execute("SELECT * FROM donors WHERE id=?", (linked["donor_id"],)).fetchone()
                    if linked["status"] not in {"withdrawn", "expired", "implanted"}:
                        self._expire_allocation(conn, linked, donor, actor, role)
                    self._release_hold(conn, head["id"], "organ_expired", actor, role)
                    continue
            conn.execute("UPDATE slot_holds SET status='held',held_at=? WHERE id=?", (iso(), head["id"]))
            Repository.audit(conn, head["allocation_id"], None, actor, role, "slot_promoted",
                             {"slot_id": slot["id"], "hold_id": head["id"], "allocation_id": head["allocation_id"]})

    # ----- allocation flow -------------------------------------------------------

    @staticmethod
    def _score(donor: sqlite3.Row, candidate: sqlite3.Row) -> dict[str, float]:
        region = 250 if donor["region"] == candidate["region"] else 0
        clinical = min(donor["clinical_match"], candidate["clinical_match"]) * 30
        return {"urgency": candidate["urgency"] * 1000, "waiting": candidate["wait_days"] * 2, "region": region, "clinical": clinical,
                "total": candidate["urgency"] * 1000 + candidate["wait_days"] * 2 + region + clinical}

    def ranking(self, donor_id: int, role: str, hospital: str) -> dict[str, Any]:
        if role not in {"allocation_officer", "auditor"}: raise ApiError(403, "ranking_forbidden", "只有分配员或审计员可以查看完整候选排序")
        with self.repo.tx() as conn:
            donor = conn.execute("SELECT * FROM donors WHERE id=?", (donor_id,)).fetchone()
            if not donor: raise ApiError(404, "donor_not_found", "器官不存在")
            rows = []
            for candidate in conn.execute("SELECT * FROM candidates WHERE organ=? AND status='active' AND willing=1", (donor["organ"],)):
                if blood_compatible(donor["blood_type"], candidate["blood_type"]):
                    item = dict(candidate); item["match"] = self._score(donor, candidate); rows.append(item)
            rows.sort(key=lambda item: (-item["match"]["total"], item["id"]))
            for index, item in enumerate(rows, 1): item["rank"] = index
            return {"donor": dict(donor), "candidates": rows}

    @staticmethod
    def _find_retry(conn: sqlite3.Connection, token: str) -> int | None:
        row = conn.execute("SELECT allocation_id FROM slot_holds WHERE client_token=? AND allocation_id IS NOT NULL", (token,)).fetchone()
        if row: return row[0]
        row = conn.execute("SELECT id FROM allocations WHERE client_token=?", (token,)).fetchone()
        return row[0] if row else None

    def propose(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "allocation_officer": raise ApiError(403, "allocate_forbidden", "只有分配员可以提出分配")
        donor_id, candidate_id = body.get("donor_id"), body.get("candidate_id")
        if not isinstance(donor_id, int) or not isinstance(candidate_id, int): raise ApiError(400, "ids_required", "donor_id 和 candidate_id 必填")
        token_raw = body.get("client_token")
        token = token_raw.strip() if isinstance(token_raw, str) and token_raw.strip() else None
        if token is not None and len(token) > 64: raise ApiError(400, "invalid_client_token", "client_token 最长 64 字符")
        day = parse_slot_date(body["slot_date"]) if body.get("slot_date") else utcnow().date()

        # Phase A: validate everything and reserve the surgery slot. The hold is committed on its own so that a
        # later persistence failure keeps the position and a retry with the same client_token reuses it.
        with self.repo.tx() as conn:
            if token:
                retry_id = self._find_retry(conn, token)
                if retry_id is not None:
                    return self._allocation(conn, retry_id, role, "")
            donor = conn.execute("SELECT * FROM donors WHERE id=?", (donor_id,)).fetchone(); candidate = conn.execute("SELECT * FROM candidates WHERE id=?", (candidate_id,)).fetchone()
            if not donor or not candidate: raise ApiError(404, "not_found", "器官或候选患者不存在")
            if donor["status"] != "available": raise ApiError(409, "donor_unavailable", "器官当前不可分配")
            if parse_time(donor["expires_at"]) <= utcnow():
                conn.execute("UPDATE donors SET status='expired',revision=revision+1 WHERE id=?", (donor_id,))
                Repository.audit(conn, None, donor_id, actor, role, "organ_expired", {"candidate_id": candidate_id})
                raise ApiError(409, "organ_expired", "器官可用窗口已结束")
            if candidate["status"] != "active" or not candidate["willing"]: raise ApiError(409, "candidate_unavailable", "候选患者当前不可接受分配")
            if donor["organ"] != candidate["organ"] or not blood_compatible(donor["blood_type"], candidate["blood_type"]):
                raise ApiError(409, "medical_mismatch", "器官类型或血型不匹配")
            if conn.execute("SELECT 1 FROM allocations WHERE donor_id=? AND status NOT IN ('withdrawn','expired')", (donor_id,)).fetchone():
                raise ApiError(409, "already_allocated", "该器官已有有效分配")
            if day.isoformat() > donor["expires_at"][:10]:
                raise ApiError(400, "slot_after_expiry", "手术日期晚于器官可用窗口，无法占位")
            slot = conn.execute("SELECT * FROM surgery_slots WHERE hospital=? AND slot_date=?", (candidate["hospital"], day.isoformat())).fetchone()
            hold_id: int | None = None
            if slot:
                orphan = conn.execute("SELECT * FROM slot_holds WHERE client_token=? AND allocation_id IS NULL", (token,)).fetchone() if token else None
                if orphan:
                    hold_id = orphan["id"]  # earlier attempt reserved this position; never insert a second hold
                else:
                    self._sweep_expired_holds(conn, slot, actor, role)
                    self._promote_queue(conn, slot, actor, role)
                    slot = conn.execute("SELECT * FROM surgery_slots WHERE id=?", (slot["id"],)).fetchone()
                    remaining = self._capacity(slot) - self._occupied(conn, slot["id"])
                    if remaining > 0:
                        cur = conn.execute("""INSERT INTO slot_holds(slot_id,client_token,status,held_at,created_at)
                                              VALUES(?,?, 'held', ?,?)""", (slot["id"], token, iso(), iso()))
                        hold_id = cur.lastrowid
                        Repository.audit(conn, None, donor_id, actor, role, "slot_held",
                                         {"hold_id": hold_id, "slot_date": slot["slot_date"], "hospital": candidate["hospital"], "remaining": remaining - 1})
                    else:
                        cur = conn.execute("""INSERT INTO slot_holds(slot_id,client_token,status,enqueued_at,created_at)
                                              VALUES(?,?, 'queued', ?,?)""", (slot["id"], token, iso(), iso()))
                        hold_id = cur.lastrowid
                        Repository.audit(conn, None, donor_id, actor, role, "slot_queued",
                                         {"hold_id": hold_id, "slot_date": slot["slot_date"], "hospital": candidate["hospital"],
                                          "queue_position": self._queue_position(conn, hold_id)})
            score = self._score(donor, candidate)

        # Phase B: persist the allocation itself and bind it to the reserved hold.
        try:
            with self.repo.tx() as conn:
                if self.fault_on_next_allocation_insert:
                    self.fault_on_next_allocation_insert = False
                    raise sqlite3.OperationalError("injected persistence fault")
                cur = conn.execute("""INSERT INTO allocations(donor_id,candidate_id,score,created_by,created_at,updated_at,hold_id,client_token)
                                      VALUES(?,?,?,?,?,?,?,?)""",
                                   (donor_id, candidate_id, score["total"], actor, iso(), iso(), hold_id, token))
                allocation_id = cur.lastrowid
                if hold_id is not None:
                    conn.execute("UPDATE slot_holds SET allocation_id=? WHERE id=?", (allocation_id, hold_id))
                conn.execute("UPDATE donors SET status='allocated',revision=revision+1 WHERE id=?", (donor_id,))
                Repository.audit(conn, allocation_id, donor_id, actor, role, "allocation_proposed", {"candidate_id": candidate_id, "score": score})
                return self._allocation(conn, allocation_id, role, "")
        except sqlite3.IntegrityError as exc:  # donor was taken by a concurrent request between A and B
            if hold_id is not None:
                with self.repo.tx() as conn:
                    self._release_hold(conn, hold_id, "allocation_conflict", actor, role)
                    slot = conn.execute("SELECT * FROM surgery_slots WHERE id=(SELECT slot_id FROM slot_holds WHERE id=?)", (hold_id,)).fetchone()
                    if slot: self._promote_queue(conn, slot, actor, role)
            raise ApiError(409, "already_allocated", "该器官已被其他分配占用") from exc
        except sqlite3.Error as exc:
            if hold_id is not None and token:
                details: dict[str, Any] = {"client_token": token, "hold_id": hold_id}
                with self.repo.tx() as conn:
                    hold = conn.execute("SELECT * FROM slot_holds WHERE id=?", (hold_id,)).fetchone()
                    if hold:
                        details["slot"] = self._hold_summary(conn, hold)
                raise ApiError(500, "persistence_failed", "分配单入库失败，已保留占位，使用相同 client_token 重试即可", details) from exc
            if hold_id is not None:
                with self.repo.tx() as conn:
                    self._release_hold(conn, hold_id, "persistence_failed", actor, role)
            raise ApiError(500, "internal_error", f"分配单入库失败: {exc}") from exc

    def _allocation(self, conn: sqlite3.Connection, allocation_id: int, role: str, hospital: str) -> dict[str, Any]:
        row = conn.execute("""SELECT a.*,d.blood_type donor_blood,d.organ,d.hospital donor_hospital,d.region donor_region,d.available_at,d.expires_at,d.status donor_status,
                                     c.patient_name,c.blood_type candidate_blood,c.hospital candidate_hospital,c.region candidate_region,c.urgency,c.wait_days
                              FROM allocations a JOIN donors d ON d.id=a.donor_id JOIN candidates c ON c.id=a.candidate_id WHERE a.id=?""", (allocation_id,)).fetchone()
        if not row: raise ApiError(404, "allocation_not_found", "分配不存在")
        result = dict(row)
        if role == "hospital" and hospital not in {row["donor_hospital"], row["candidate_hospital"]}:
            raise ApiError(403, "allocation_forbidden", "医院不能查看与本机构无关的分配")
        if role == "hospital" and hospital != row["candidate_hospital"]:
            result["patient_name"] = "***"
        result["handoff"] = self._row(conn.execute("SELECT * FROM handoffs WHERE allocation_id=?", (allocation_id,)).fetchone())
        result["slot"] = None
        if row["hold_id"]:
            hold = conn.execute("SELECT * FROM slot_holds WHERE id=?", (row["hold_id"],)).fetchone()
            if hold: result["slot"] = self._hold_summary(conn, hold)
        return result

    def _lifecycle_preflight(self, allocation_id: int, actor: str, role: str, settle_queue: bool = False) -> None:
        """Commit lazy expiry / queue housekeeping in its own transaction, because the caller rejects
        the attempted action afterwards and a rollback would discard the released slot."""
        elapsed = False
        with self.repo.tx() as conn:
            row = conn.execute("SELECT * FROM allocations WHERE id=?", (allocation_id,)).fetchone()
            if not row or row["status"] in {"withdrawn", "expired", "implanted"}: return
            donor = conn.execute("SELECT * FROM donors WHERE id=?", (row["donor_id"],)).fetchone()
            if parse_time(donor["expires_at"]) <= utcnow():
                self._expire_allocation(conn, row, donor, actor, role)
                if row["hold_id"]:
                    self._release_hold(conn, row["hold_id"], "organ_expired", actor, role)
                    slot = conn.execute("SELECT * FROM surgery_slots WHERE id=(SELECT slot_id FROM slot_holds WHERE id=?)", (row["hold_id"],)).fetchone()
                    if slot: self._promote_queue(conn, slot, actor, role)
                elapsed = True
            elif settle_queue and row["hold_id"]:
                hold = conn.execute("SELECT * FROM slot_holds WHERE id=?", (row["hold_id"],)).fetchone()
                if hold and hold["status"] == "queued":
                    slot = conn.execute("SELECT * FROM surgery_slots WHERE id=?", (hold["slot_id"],)).fetchone()
                    self._sweep_expired_holds(conn, slot, actor, role)
                    self._promote_queue(conn, slot, actor, role)
        if elapsed: raise ApiError(409, "organ_expired", "器官已经超过可用时间，禁止继续流转")

    def _ensure_active(self, conn: sqlite3.Connection, allocation_id: int) -> sqlite3.Row:
        row = conn.execute("SELECT * FROM allocations WHERE id=?", (allocation_id,)).fetchone()
        if not row: raise ApiError(404, "allocation_not_found", "分配不存在")
        if row["status"] in {"withdrawn", "expired", "implanted"}: raise ApiError(409, "allocation_closed", "分配已结束")
        donor = conn.execute("SELECT * FROM donors WHERE id=?", (row["donor_id"],)).fetchone()
        if parse_time(donor["expires_at"]) <= utcnow():
            raise ApiError(409, "organ_expired", "器官已经超过可用时间，禁止继续流转")  # committed by preflight
        return row

    def accept(self, allocation_id: int, actor: str, role: str, hospital: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "hospital": raise ApiError(403, "hospital_required", "只有接收医院可以接受器官")
        expected = body.get("expected_revision")
        if not isinstance(expected, int): raise ApiError(400, "revision_required", "expected_revision 必填")
        peek = self.repo.conn.execute("SELECT c.hospital FROM allocations a JOIN candidates c ON c.id=a.candidate_id WHERE a.id=?", (allocation_id,)).fetchone()
        if peek and peek["hospital"] != hospital: raise ApiError(403, "wrong_hospital", "只能由候选患者所在医院接受")
        self._lifecycle_preflight(allocation_id, actor, role, settle_queue=True)
        with self.repo.tx() as conn:
            row = self._ensure_active(conn, allocation_id)
            candidate = conn.execute("SELECT * FROM candidates WHERE id=?", (row["candidate_id"],)).fetchone()
            if candidate["hospital"] != hospital: raise ApiError(403, "wrong_hospital", "只能由候选患者所在医院接受")
            if row["status"] == "accepted": return self._allocation(conn, allocation_id, role, hospital)
            if row["status"] != "proposed": raise ApiError(409, "invalid_transition", "当前状态不能接受")
            if row["revision"] != expected: raise ApiError(409, "revision_conflict", "分配信息已发生变化")
            if row["hold_id"]:
                hold = conn.execute("SELECT * FROM slot_holds WHERE id=?", (row["hold_id"],)).fetchone()
                if hold and hold["status"] == "queued":
                    raise ApiError(409, "slot_pending", "手术间仍在排队，尚未占位，不能接受", self._hold_summary(conn, hold))
            conn.execute("UPDATE allocations SET status='accepted',accepted_at=?,revision=revision+1,updated_at=? WHERE id=?", (iso(), iso(), allocation_id))
            Repository.audit(conn, allocation_id, row["donor_id"], actor, role, "allocation_accepted", {"hospital": hospital})
            return self._allocation(conn, allocation_id, role, hospital)

    def mark_transit(self, allocation_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "allocation_officer": raise ApiError(403, "transit_forbidden", "只有分配员可以登记转运")
        temp = body.get("cold_chain_temp")
        if not isinstance(temp, (int, float)) or not -2 <= float(temp) <= 8:
            raise ApiError(409, "cold_chain_violation", "冷链温度必须保持在 -2°C 到 8°C")
        self._lifecycle_preflight(allocation_id, actor, role)
        with self.repo.tx() as conn:
            row = self._ensure_active(conn, allocation_id)
            if row["status"] != "accepted": raise ApiError(409, "invalid_transition", "只有已接受分配可以进入转运")
            conn.execute("UPDATE allocations SET status='in_transit',cold_chain_temp=?,revision=revision+1,updated_at=? WHERE id=?", (float(temp), iso(), allocation_id))
            Repository.audit(conn, allocation_id, row["donor_id"], actor, role, "transfer_started", {"cold_chain_temp": temp})
            return self._allocation(conn, allocation_id, role, "")

    def report_delay(self, allocation_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"allocation_officer", "hospital"}: raise ApiError(403, "delay_forbidden", "当前角色不能上报延误")
        minutes, reason = body.get("delayed_minutes"), str(body.get("reason", "")).strip()
        if not isinstance(minutes, int) or minutes <= 0 or not reason: raise ApiError(400, "invalid_delay", "delayed_minutes 必须为正整数且 reason 必填")
        self._lifecycle_preflight(allocation_id, actor, role)
        with self.repo.tx() as conn:
            row = self._ensure_active(conn, allocation_id)
            conn.execute("UPDATE allocations SET delayed_minutes=delayed_minutes+?,revision=revision+1,updated_at=? WHERE id=?", (minutes, iso(), allocation_id))
            Repository.audit(conn, allocation_id, row["donor_id"], actor, role, "logistics_delay", {"minutes": minutes, "reason": reason, "at_risk": minutes >= 120})
            return self._allocation(conn, allocation_id, role, "")

    def initiate_handoff(self, allocation_id: int, actor: str, role: str, hospital: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "hospital": raise ApiError(403, "handoff_forbidden", "只有医院可以发起交接")
        expected = body.get("expected_revision"); target = str(body.get("to_hospital", "")).strip(); temp = body.get("cold_chain_temp")
        if not isinstance(expected, int) or not target or not isinstance(temp, (int, float)) or not -2 <= float(temp) <= 8:
            raise ApiError(400, "invalid_handoff", "expected_revision、to_hospital 和合规冷链温度必填")
        self._lifecycle_preflight(allocation_id, actor, role)
        with self.repo.tx() as conn:
            row = self._ensure_active(conn, allocation_id)
            donor = conn.execute("SELECT * FROM donors WHERE id=?", (row["donor_id"],)).fetchone()
            candidate = conn.execute("SELECT * FROM candidates WHERE id=?", (row["candidate_id"],)).fetchone()
            if hospital != donor["hospital"]: raise ApiError(403, "wrong_hospital", "只能由器官来源医院发起交接")
            if target != candidate["hospital"]: raise ApiError(409, "wrong_destination", "交接目标必须与候选患者医院一致")
            if row["status"] != "in_transit": raise ApiError(409, "invalid_transition", "只有转运中分配可以交接")
            if row["revision"] != expected: raise ApiError(409, "revision_conflict", "分配版本已变化")
            try:
                cur = conn.execute("""INSERT INTO handoffs(allocation_id,from_hospital,to_hospital,cold_chain_temp,initiated_by,initiated_at)
                                      VALUES(?,?,?,?,?,?)""", (allocation_id, hospital, target, float(temp), actor, iso()))
            except sqlite3.IntegrityError as exc:
                raise ApiError(409, "handoff_exists", "交接已经登记") from exc
            Repository.audit(conn, allocation_id, row["donor_id"], actor, role, "handoff_initiated", {"target": target, "cold_chain_temp": temp})
            return {"handoff": dict(conn.execute("SELECT * FROM handoffs WHERE id=?", (cur.lastrowid,)).fetchone()), "allocation": self._allocation(conn, allocation_id, role, hospital)}

    def accept_handoff(self, allocation_id: int, actor: str, role: str, hospital: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "hospital": raise ApiError(403, "handoff_forbidden", "只有医院可以确认交接")
        self._lifecycle_preflight(allocation_id, actor, role)
        with self.repo.tx() as conn:
            row = self._ensure_active(conn, allocation_id)
            handoff = conn.execute("SELECT * FROM handoffs WHERE allocation_id=?", (allocation_id,)).fetchone()
            if not handoff: raise ApiError(409, "handoff_missing", "尚未发起交接")
            if handoff["to_hospital"] != hospital: raise ApiError(403, "wrong_hospital", "只能由接收医院确认交接")
            if handoff["status"] == "accepted": return self._allocation(conn, allocation_id, role, hospital)
            conn.execute("UPDATE handoffs SET status='accepted',accepted_by=?,accepted_at=? WHERE id=?", (actor, iso(), handoff["id"]))
            conn.execute("UPDATE allocations SET status='handed_off',revision=revision+1,updated_at=? WHERE id=?", (iso(), allocation_id))
            Repository.audit(conn, allocation_id, row["donor_id"], actor, role, "handoff_accepted", {"handoff_id": handoff["id"]})
            return self._allocation(conn, allocation_id, role, hospital)

    def implant(self, allocation_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "allocation_officer": raise ApiError(403, "implant_forbidden", "只有分配员可以确认植入")
        self._lifecycle_preflight(allocation_id, actor, role)
        with self.repo.tx() as conn:
            row = self._ensure_active(conn, allocation_id)
            if row["status"] != "handed_off": raise ApiError(409, "invalid_transition", "交接完成后才能确认植入")
            conn.execute("UPDATE allocations SET status='implanted',implanted_at=?,revision=revision+1,updated_at=? WHERE id=?", (iso(), iso(), allocation_id))
            conn.execute("UPDATE donors SET status='used',revision=revision+1 WHERE id=?", (row["donor_id"],))
            if row["hold_id"]:
                conn.execute("UPDATE slot_holds SET status='consumed' WHERE id=? AND status='held'", (row["hold_id"],))
            Repository.audit(conn, allocation_id, row["donor_id"], actor, role, "organ_implanted", {"candidate_id": row["candidate_id"]})
            return self._allocation(conn, allocation_id, role, "")

    def withdraw(self, allocation_id: int, actor: str, role: str, hospital: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "hospital": raise ApiError(403, "withdraw_forbidden", "只有医院可以撤回")
        reason = str(body.get("reason", "")).strip()
        if not reason: raise ApiError(400, "reason_required", "撤回原因必填")
        self._lifecycle_preflight(allocation_id, actor, role)
        with self.repo.tx() as conn:
            row = self._ensure_active(conn, allocation_id)
            candidate = conn.execute("SELECT * FROM candidates WHERE id=?", (row["candidate_id"],)).fetchone()
            if hospital != candidate["hospital"]: raise ApiError(403, "wrong_hospital", "只能由候选患者医院撤回")
            if row["status"] not in {"proposed", "accepted"}: raise ApiError(409, "invalid_transition", "转运开始后不能直接撤回")
            conn.execute("UPDATE allocations SET status='withdrawn',revision=revision+1,updated_at=? WHERE id=?", (iso(), allocation_id))
            donor = conn.execute("SELECT * FROM donors WHERE id=?", (row["donor_id"],)).fetchone()
            donor_status = "available" if parse_time(donor["expires_at"]) > utcnow() else "expired"
            conn.execute("UPDATE donors SET status=?,revision=revision+1 WHERE id=?", (donor_status, row["donor_id"]))
            if row["hold_id"]:
                self._release_hold(conn, row["hold_id"], "withdrawn", actor, role)
                slot = conn.execute("SELECT * FROM surgery_slots WHERE id=(SELECT slot_id FROM slot_holds WHERE id=?)", (row["hold_id"],)).fetchone()
                if slot: self._promote_queue(conn, slot, actor, role)
            Repository.audit(conn, allocation_id, row["donor_id"], actor, role, "allocation_withdrawn", {"reason": reason})
            return self._allocation(conn, allocation_id, role, hospital)

    def get_allocation(self, allocation_id: int, role: str, hospital: str) -> dict[str, Any]:
        return self._allocation(self.repo.conn, allocation_id, role, hospital)

    def audit(self, allocation_id: int, role: str) -> list[dict[str, Any]]:
        if role not in {"auditor", "allocation_officer"}: raise ApiError(403, "audit_forbidden", "当前角色不能查看审计记录")
        return [dict(r) for r in self.repo.conn.execute("SELECT actor,role,action,detail_json,created_at FROM audit_log WHERE allocation_id=? ORDER BY id", (allocation_id,))]

    def state(self, role: str, hospital: str) -> dict[str, Any]:
        conn = self.repo.conn
        if role == "hospital":
            donors = [dict(r) for r in conn.execute("SELECT * FROM donors WHERE hospital=?", (hospital,))]
            candidates = [dict(r) for r in conn.execute("SELECT * FROM candidates WHERE hospital=?", (hospital,))]
            allocated = [dict(r) for r in conn.execute("SELECT a.* FROM allocations a JOIN candidates c ON c.id=a.candidate_id WHERE c.hospital=?", (hospital,))]
        elif role == "viewer":
            donors = []
            candidates = []
            allocated = [dict(r) for r in conn.execute("SELECT id,status,updated_at FROM allocations WHERE status='implanted' ORDER BY id DESC")]
        else:
            donors = [dict(r) for r in conn.execute("SELECT * FROM donors ORDER BY id DESC")]
            candidates = [dict(r) for r in conn.execute("SELECT * FROM candidates ORDER BY id DESC")]
            allocated = [dict(r) for r in conn.execute("SELECT * FROM allocations ORDER BY id DESC")]
        return {"donors": donors, "candidates": candidates, "allocations": allocated,
                "slots": self._slots_data(conn, role, hospital), "server_time": iso()}


def json_reply(handler: BaseHTTPRequestHandler, status: int, payload: Any) -> None:
    raw = json.dumps(payload, ensure_ascii=False, default=str).encode(); handler.send_response(status); handler.send_header("Content-Type", "application/json; charset=utf-8"); handler.send_header("Content-Length", str(len(raw))); handler.end_headers(); handler.wfile.write(raw)


class Handler(BaseHTTPRequestHandler):
    service: OrganAllocationService; web_root: Path
    def log_message(self, fmt: str, *args: Any) -> None: print(f"{self.address_string()} - {fmt % args}")
    def read_body(self) -> dict[str, Any]:
        size = int(self.headers.get("Content-Length", "0"))
        if not size: return {}
        try: body = json.loads(self.rfile.read(size))
        except json.JSONDecodeError as exc: raise ApiError(400, "invalid_json", "请求体不是有效 JSON") from exc
        if not isinstance(body, dict): raise ApiError(400, "invalid_json", "请求体必须是对象")
        return body
    def dispatch_get(self, path: str) -> tuple[int, Any]:
        if path == "/health": return 200, {"status": "ok", "service": "organ-allocation"}
        actor, role, hospital = self.service.identity(self.headers)
        if path == "/api/state": return 200, self.service.state(role, hospital)
        if path == "/api/slots": return 200, self.service.slots(role, hospital)
        parts = [p for p in path.split("/") if p]
        if len(parts) == 4 and parts[:2] == ["api", "donors"] and parts[2].isdigit() and parts[3] == "ranking": return 200, self.service.ranking(int(parts[2]), role, hospital)
        if len(parts) == 3 and parts[:2] == ["api", "allocations"] and parts[2].isdigit(): return 200, self.service.get_allocation(int(parts[2]), role, hospital)
        if len(parts) == 4 and parts[:2] == ["api", "allocations"] and parts[2].isdigit() and parts[3] == "audit": return 200, {"audit": self.service.audit(int(parts[2]), role)}
        raise ApiError(404, "not_found", "接口不存在")
    def dispatch_post(self, path: str) -> tuple[int, Any]:
        actor, role, hospital = self.service.identity(self.headers); body = self.read_body(); parts = [p for p in path.split("/") if p]
        actions = {
            "/api/donors": lambda: (201, self.service.register_donor(actor, role, body)),
            "/api/candidates": lambda: (201, self.service.register_candidate(actor, role, body)),
            "/api/allocations": lambda: (201, self.service.propose(actor, role, body)),
            "/api/slots": lambda: (201, self.service.register_slot(actor, role, hospital, body)),
        }
        if path in actions: return actions[path]()
        if len(parts) == 4 and parts[:2] == ["api", "allocations"] and parts[2].isdigit():
            aid, action = int(parts[2]), parts[3]
            routes = {
                "accept": lambda: self.service.accept(aid, actor, role, hospital, body),
                "withdraw": lambda: self.service.withdraw(aid, actor, role, hospital, body),
                "transit": lambda: self.service.mark_transit(aid, actor, role, body),
                "delay": lambda: self.service.report_delay(aid, actor, role, body),
                "handoff": lambda: self.service.initiate_handoff(aid, actor, role, hospital, body),
                "handoff-accept": lambda: self.service.accept_handoff(aid, actor, role, hospital, body),
                "implant": lambda: self.service.implant(aid, actor, role, body),
            }
            if action in routes: return 200, routes[action]()
        raise ApiError(404, "not_found", "接口不存在")
    def handle_any(self, method: str) -> None:
        parsed = urlparse(self.path)
        try:
            if method == "GET" and parsed.path == "/":
                raw = (self.web_root / "index.html").read_bytes(); self.send_response(200); self.send_header("Content-Type", "text/html; charset=utf-8"); self.send_header("Content-Length", str(len(raw))); self.end_headers(); self.wfile.write(raw); return
            status, payload = self.dispatch_get(parsed.path) if method == "GET" else self.dispatch_post(parsed.path)
            json_reply(self, status, payload)
        except ApiError as exc: json_reply(self, exc.status, {"error": exc.code, "message": exc.message, **exc.details})
        except Exception as exc: print(f"unhandled error: {exc!r}"); json_reply(self, 500, {"error": "internal_error", "message": str(exc)})
    def do_GET(self) -> None: self.handle_any("GET")
    def do_POST(self) -> None: self.handle_any("POST")


def create_server(db_path: str | Path, host: str = "127.0.0.1", port: int = PORT) -> ThreadingHTTPServer:
    service = OrganAllocationService(db_path); handler = type("OrganHandler", (Handler,), {"service": service, "web_root": Path(__file__).resolve().parent / "static"}); return ThreadingHTTPServer((host, port), handler)


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--host", default="127.0.0.1"); parser.add_argument("--port", type=int, default=PORT); parser.add_argument("--db", default=os.environ.get("ORGAN_DB", "organ_allocation.db")); args = parser.parse_args()
    server = create_server(args.db, args.host, args.port); print(f"organ-allocation listening on http://{args.host}:{args.port}")
    try: server.serve_forever()
    except KeyboardInterrupt: pass
    finally: server.server_close()

if __name__ == "__main__": main()
