#!/usr/bin/env python3
"""Organ allocation and cold-chain coordination service (standard library only)."""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

PORT = 8203
ROLES = {"viewer", "hospital", "coordinator", "allocation_officer", "auditor"}
STATUSES = {"proposed", "accepted", "in_transit", "handed_off", "implanted", "withdrawn", "expired"}


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(message); self.status, self.code, self.message = status, code, message


def utcnow() -> datetime: return datetime.now(timezone.utc)
def iso(value: datetime | None = None) -> str: return (value or utcnow()).replace(microsecond=0).isoformat().replace("+00:00", "Z")
def parse_time(value: str | None) -> datetime:
    if not value: raise ApiError(400, "time_required", "必须提供 ISO 8601 时间")
    try: parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc: raise ApiError(400, "invalid_time", f"时间格式错误: {value}") from exc
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def blood_compatible(donor: str, recipient: str) -> bool:
    return {
        "O": {"O", "A", "B", "AB"}, "A": {"A", "AB"}, "B": {"B", "AB"}, "AB": {"AB"},
    }.get(donor.upper(), set()) and recipient.upper() in {"O", "A", "B", "AB"} and recipient.upper() in {
        "O": {"O", "A", "B", "AB"}, "A": {"A", "AB"}, "B": {"B", "AB"}, "AB": {"AB"},
    }.get(donor.upper(), set())


class Repository:
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
        CREATE TABLE IF NOT EXISTS allocations(
            id INTEGER PRIMARY KEY AUTOINCREMENT, donor_id INTEGER NOT NULL UNIQUE REFERENCES donors(id), candidate_id INTEGER NOT NULL REFERENCES candidates(id),
            score REAL NOT NULL, status TEXT NOT NULL DEFAULT 'proposed', revision INTEGER NOT NULL DEFAULT 1,
            cold_chain_temp REAL, delayed_minutes INTEGER NOT NULL DEFAULT 0, created_by TEXT NOT NULL, created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL, accepted_at TEXT, implanted_at TEXT, client_token TEXT UNIQUE
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
        CREATE TABLE IF NOT EXISTS or_slots(
            id INTEGER PRIMARY KEY AUTOINCREMENT, hospital TEXT NOT NULL, slot_date TEXT NOT NULL, period TEXT NOT NULL DEFAULT 'DAY',
            capacity INTEGER NOT NULL, created_by TEXT NOT NULL, created_at TEXT NOT NULL, UNIQUE(hospital, slot_date, period)
        );
        CREATE TABLE IF NOT EXISTS or_holds(
            id INTEGER PRIMARY KEY AUTOINCREMENT, slot_id INTEGER NOT NULL REFERENCES or_slots(id),
            allocation_id INTEGER UNIQUE REFERENCES allocations(id), client_token TEXT UNIQUE,
            status TEXT NOT NULL DEFAULT 'held', created_at TEXT NOT NULL, released_at TEXT
        );
        CREATE TABLE IF NOT EXISTS or_queue(
            id INTEGER PRIMARY KEY AUTOINCREMENT, allocation_id INTEGER NOT NULL UNIQUE REFERENCES allocations(id),
            hospital TEXT NOT NULL, position INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'waiting',
            created_at TEXT NOT NULL
        );
        """

    def __init__(self, path: str | Path):
        self._path = str(path)
        self._local = threading.local()
        setup = self._connect()
        setup.executescript(self.SCHEMA)
        cols = {row[1] for row in setup.execute("PRAGMA table_info(allocations)")}
        if "client_token" not in cols:
            setup.execute("ALTER TABLE allocations ADD COLUMN client_token TEXT")
        setup.close()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path, check_same_thread=False, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=10000")
        return conn

    @property
    def conn(self) -> sqlite3.Connection:
        cached = getattr(self._local, "conn", None)
        if cached is None:
            cached = self._connect(); self._local.conn = cached
        return cached

    @contextmanager
    def tx(self):
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn; conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK"); raise

    @staticmethod
    def audit(conn: sqlite3.Connection, allocation_id: int | None, donor_id: int | None, actor: str, role: str, action: str, detail: dict[str, Any]) -> None:
        conn.execute("INSERT INTO audit_log(allocation_id,donor_id,actor,role,action,detail_json,created_at) VALUES(?,?,?,?,?,?,?)",
                     (allocation_id, donor_id, actor, role, action, json.dumps(detail, ensure_ascii=False, sort_keys=True), iso()))


class OrganAllocationService:
    def __init__(self, path: str | Path): self.repo = Repository(path)

    @staticmethod
    def identity(headers: Any) -> tuple[str, str, str]:
        actor, role, hospital = headers.get("X-User-Id", "").strip(), headers.get("X-Role", "").strip(), headers.get("X-Hospital", "").strip()
        if not actor or role not in ROLES: raise ApiError(401, "unauthorized", "需要 X-User-Id 和有效 X-Role")
        if role == "hospital" and not hospital: raise ApiError(401, "hospital_required", "医院角色必须提供 X-Hospital")
        return actor, role, hospital

    @staticmethod
    def _row(row: sqlite3.Row | None) -> dict[str, Any] | None: return dict(row) if row else None

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

    # ---------- 手术间排期与容量 ----------
    @staticmethod
    def _held_count_sql() -> str:
        return "(SELECT COUNT(*) FROM or_holds h WHERE h.slot_id=s.id AND h.status='held')"

    def _hold_info(self, conn: sqlite3.Connection, allocation_id: int) -> dict[str, Any] | None:
        row = conn.execute("""SELECT h.id hold_id,h.status hold_status,h.created_at hold_created_at,h.released_at,
                                     s.id slot_id,s.hospital,s.slot_date,s.period,s.capacity
                              FROM or_holds h JOIN or_slots s ON s.id=h.slot_id WHERE h.allocation_id=?""",
                           (allocation_id,)).fetchone()
        if not row: return None
        info = dict(row)
        info["held"] = conn.execute("SELECT COUNT(*) c FROM or_holds WHERE slot_id=? AND status='held'",
                                    (info["slot_id"],)).fetchone()["c"]
        info["remaining"] = info["capacity"] - info["held"]
        return info

    def _capacity(self, conn: sqlite3.Connection, hospital: str) -> sqlite3.Row | str | None:
        """最早有余量的时段；已登记但全满返回 'FULL'；未登记任何时段返回 None（不校验容量）。"""
        rows = conn.execute(f"SELECT s.*, {self._held_count_sql()} held FROM or_slots s WHERE s.hospital=? "
                            "ORDER BY s.slot_date,s.period,s.id", (hospital,)).fetchall()
        if not rows: return None
        today = iso()[:10]
        for row in rows:
            if row["slot_date"] >= today and row["capacity"] - row["held"] > 0:
                return row
        return "FULL"

    def _release_hold(self, conn: sqlite3.Connection, allocation_id: int) -> None:
        conn.execute("UPDATE or_holds SET status='released',released_at=? WHERE allocation_id=? AND status='held'",
                     (iso(), allocation_id))

    def _promote_waiting(self, conn: sqlite3.Connection, hospital: str) -> int | None:
        row = conn.execute("SELECT * FROM or_queue WHERE hospital=? AND status='waiting' ORDER BY position,id LIMIT 1",
                           (hospital,)).fetchone()
        if not row: return None
        slot = self._capacity(conn, hospital)
        if not isinstance(slot, sqlite3.Row): return None
        conn.execute("INSERT INTO or_holds(slot_id,allocation_id,client_token,status,created_at) VALUES(?,?,NULL,'held',?)",
                     (slot["id"], row["allocation_id"], iso()))
        conn.execute("UPDATE allocations SET status='proposed',revision=revision+1,updated_at=? WHERE id=?",
                     (iso(), row["allocation_id"]))
        conn.execute("UPDATE or_queue SET status='promoted' WHERE id=?", (row["id"],))
        Repository.audit(conn, row["allocation_id"], None, "system", "coordinator", "allocation_promoted_from_queue",
                         {"slot_id": slot["id"], "position": row["position"]})
        return row["allocation_id"]

    def _sweep_expired(self, conn: sqlite3.Connection) -> None:
        now = iso()
        active = conn.execute("""SELECT a.id allocation_id,d.id donor_id,c.hospital hospital
                                 FROM allocations a JOIN donors d ON d.id=a.donor_id
                                 JOIN candidates c ON c.id=a.candidate_id
                                 WHERE a.status IN ('proposed','accepted','in_transit','handed_off') AND d.expires_at<=?""",
                              (now,)).fetchall()
        for row in active:
            conn.execute("UPDATE allocations SET status='expired',revision=revision+1,updated_at=? WHERE id=?",
                         (now, row["allocation_id"]))
            conn.execute("UPDATE donors SET status='expired',revision=revision+1 WHERE id=?", (row["donor_id"],))
            self._release_hold(conn, row["allocation_id"])
            Repository.audit(conn, row["allocation_id"], row["donor_id"], "system", "coordinator",
                             "allocation_expired", {"reason": "organ_window_elapsed"})
            self._promote_waiting(conn, row["hospital"])
        queued = conn.execute("""SELECT a.id allocation_id,d.id donor_id
                                 FROM allocations a JOIN donors d ON d.id=a.donor_id
                                 WHERE a.status='queued' AND d.expires_at<=?""", (now,)).fetchall()
        for row in queued:
            conn.execute("UPDATE allocations SET status='expired',revision=revision+1,updated_at=? WHERE id=?",
                         (now, row["allocation_id"]))
            conn.execute("UPDATE donors SET status='expired',revision=revision+1 WHERE id=?", (row["donor_id"],))
            conn.execute("UPDATE or_queue SET status='cancelled' WHERE allocation_id=? AND status='waiting'",
                         (row["allocation_id"],))
            Repository.audit(conn, row["allocation_id"], row["donor_id"], "system", "coordinator",
                             "allocation_expired", {"reason": "organ_window_elapsed_in_queue"})

    def register_slot(self, actor: str, role: str, hospital: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"hospital", "coordinator", "allocation_officer"}:
            raise ApiError(403, "slot_forbidden", "当前角色不能登记手术间时段")
        target_hospital = hospital if role == "hospital" else str(body.get("hospital", "")).strip()
        if role != "hospital" and not target_hospital: raise ApiError(400, "hospital_required", "必须提供医院")
        raw_date = str(body.get("slot_date", "")).strip()
        if not raw_date: raise ApiError(400, "slot_date_required", "必须提供时段日期 slot_date")
        try: slot_date = datetime.strptime(raw_date[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError as exc: raise ApiError(400, "invalid_slot_date", "slot_date 必须为 YYYY-MM-DD") from exc
        period = str(body.get("period", "DAY")).strip().upper() or "DAY"
        capacity = body.get("capacity")
        if not isinstance(capacity, int) or capacity < 0: raise ApiError(400, "invalid_capacity", "capacity 必须为非负整数")
        with self.repo.tx() as conn:
            conn.execute("""INSERT INTO or_slots(hospital,slot_date,period,capacity,created_by,created_at)
                            VALUES(?,?,?,?,?,?)
                            ON CONFLICT(hospital,slot_date,period) DO UPDATE SET capacity=excluded.capacity""",
                         (target_hospital, slot_date, period, capacity, actor, iso()))
            Repository.audit(conn, None, None, actor, role, "or_slot_registered",
                             {"hospital": target_hospital, "slot_date": slot_date, "period": period, "capacity": capacity})
            row = conn.execute(f"SELECT s.*, {self._held_count_sql()} held FROM or_slots s "
                               "WHERE s.hospital=? AND s.slot_date=? AND s.period=?",
                               (target_hospital, slot_date, period)).fetchone()
            result = dict(row); result["remaining"] = result["capacity"] - result["held"]
            return result

    def or_board(self, role: str, hospital: str) -> dict[str, Any]:
        with self.repo.tx() as conn:
            self._sweep_expired(conn)
            where, params = ("WHERE s.hospital=?", (hospital,)) if role == "hospital" else ("", ())
            slots = conn.execute(f"SELECT s.*, {self._held_count_sql()} held FROM or_slots s {where} "
                                 "ORDER BY s.hospital,s.slot_date,s.period,s.id", params).fetchall()
            qwhere = "AND c.hospital=?" if role == "hospital" else ""
            qparams = (hospital,) if role == "hospital" else ()
            queue = conn.execute(f"""SELECT q.id,q.allocation_id,q.position,q.created_at,a.status alloc_status,
                                            d.organ,d.blood_type,c.patient_name,c.hospital candidate_hospital
                                     FROM or_queue q JOIN allocations a ON a.id=q.allocation_id
                                     JOIN donors d ON d.id=a.donor_id JOIN candidates c ON c.id=a.candidate_id
                                     WHERE q.status='waiting' {qwhere} ORDER BY q.position,q.id""", qparams).fetchall()
            slot_list = []
            for row in slots:
                item = dict(row); item["remaining"] = item["capacity"] - item["held"]
                item["holds"] = [dict(h) for h in conn.execute("""SELECT h.id,h.allocation_id,h.created_at,a.status alloc_status,
                                                                        c.patient_name,d.organ,d.blood_type
                                                                 FROM or_holds h JOIN allocations a ON a.id=h.allocation_id
                                                                 JOIN donors d ON d.id=a.donor_id JOIN candidates c ON c.id=a.candidate_id
                                                                 WHERE h.slot_id=? AND h.status='held' ORDER BY h.id""", (row["id"],))]
                slot_list.append(item)
            return {"slots": slot_list, "queue": [dict(r) for r in queue], "server_time": iso()}

    def propose(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "allocation_officer": raise ApiError(403, "allocate_forbidden", "只有分配员可以提出分配")
        donor_id, candidate_id = body.get("donor_id"), body.get("candidate_id")
        if not isinstance(donor_id, int) or not isinstance(candidate_id, int): raise ApiError(400, "ids_required", "donor_id 和 candidate_id 必填")
        token = str(body.get("client_token", "")).strip() or None
        with self.repo.tx() as conn:
            self._sweep_expired(conn)
            existing_hold = None
            if token:
                existing_alloc = conn.execute("SELECT * FROM allocations WHERE client_token=?", (token,)).fetchone()
                if existing_alloc:
                    result = self._allocation(conn, existing_alloc["id"], role, "")
                    result["replayed"] = True
                    return result
                existing_hold = conn.execute("SELECT * FROM or_holds WHERE client_token=?", (token,)).fetchone()
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
            score = self._score(donor, candidate)
            capacity = self._capacity(conn, candidate["hospital"])
            if capacity is None:
                cur = conn.execute("""INSERT INTO allocations(donor_id,candidate_id,score,status,client_token,created_by,created_at,updated_at)
                                      VALUES(?,?,?,'proposed',?,?,?,?)""",
                                   (donor_id, candidate_id, score["total"], token, actor, iso(), iso()))
                allocation_id = cur.lastrowid
                conn.execute("UPDATE donors SET status='allocated',revision=revision+1 WHERE id=?", (donor_id,))
                Repository.audit(conn, allocation_id, donor_id, actor, role, "allocation_proposed", {"candidate_id": candidate_id, "score": score})
                return self._allocation(conn, allocation_id, role, "")
            if capacity == "FULL":
                cur = conn.execute("""INSERT INTO allocations(donor_id,candidate_id,score,status,client_token,created_by,created_at,updated_at)
                                      VALUES(?,?,?,'queued',?,?,?,?)""",
                                   (donor_id, candidate_id, score["total"], token, actor, iso(), iso()))
                allocation_id = cur.lastrowid
                conn.execute("UPDATE donors SET status='allocated',revision=revision+1 WHERE id=?", (donor_id,))
                position = conn.execute("SELECT COALESCE(MAX(position),0)+1 p FROM or_queue WHERE hospital=?",
                                        (candidate["hospital"],)).fetchone()["p"]
                conn.execute("INSERT INTO or_queue(allocation_id,hospital,position,status,created_at) VALUES(?,?,?,'waiting',?)",
                             (allocation_id, candidate["hospital"], position, iso()))
                Repository.audit(conn, allocation_id, donor_id, actor, role, "allocation_queued",
                                 {"candidate_id": candidate_id, "score": score, "queue_position": position, "remaining": 0})
                result = self._allocation(conn, allocation_id, role, "")
                result["remaining"] = 0; result["queue_position"] = position; result["_queued"] = True
                return result
            slot = capacity
            if existing_hold and existing_hold["status"] == "held":
                hold_id, slot_id = existing_hold["id"], existing_hold["slot_id"]
            elif existing_hold and existing_hold["status"] == "released":
                conn.execute("UPDATE or_holds SET slot_id=?,status='held',created_at=?,released_at=NULL WHERE id=?",
                             (slot["id"], iso(), existing_hold["id"]))
                hold_id, slot_id = existing_hold["id"], slot["id"]
            else:
                cur = conn.execute("INSERT INTO or_holds(slot_id,allocation_id,client_token,status,created_at) VALUES(?,?,?,'held',?)",
                                   (slot["id"], None, token, iso()))
                hold_id, slot_id = cur.lastrowid, slot["id"]
            cur = conn.execute("""INSERT INTO allocations(donor_id,candidate_id,score,status,client_token,created_by,created_at,updated_at)
                                  VALUES(?,?,?,'proposed',?,?,?,?)""",
                               (donor_id, candidate_id, score["total"], token, actor, iso(), iso()))
            allocation_id = cur.lastrowid
            conn.execute("UPDATE or_holds SET allocation_id=? WHERE id=?", (allocation_id, hold_id))
            conn.execute("UPDATE donors SET status='allocated',revision=revision+1 WHERE id=?", (donor_id,))
            Repository.audit(conn, allocation_id, donor_id, actor, role, "allocation_proposed",
                             {"candidate_id": candidate_id, "score": score, "slot_id": slot["id"]})
            result = self._allocation(conn, allocation_id, role, "")
            result["remaining"] = slot["capacity"] - (slot["held"] + 1)
            return result

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
        result["slot"] = self._hold_info(conn, allocation_id)
        return result

    def _ensure_active(self, conn: sqlite3.Connection, allocation_id: int, actor: str, role: str) -> sqlite3.Row:
        row = conn.execute("SELECT * FROM allocations WHERE id=?", (allocation_id,)).fetchone()
        if not row: raise ApiError(404, "allocation_not_found", "分配不存在")
        if row["status"] in {"withdrawn", "expired", "implanted"}: raise ApiError(409, "allocation_closed", "分配已结束")
        donor = conn.execute("SELECT * FROM donors WHERE id=?", (row["donor_id"],)).fetchone()
        if parse_time(donor["expires_at"]) <= utcnow():
            conn.execute("UPDATE allocations SET status='expired',revision=revision+1,updated_at=? WHERE id=?", (iso(), allocation_id))
            conn.execute("UPDATE donors SET status='expired',revision=revision+1 WHERE id=?", (donor["id"],))
            self._release_hold(conn, allocation_id)
            candidate = conn.execute("SELECT hospital FROM candidates WHERE id=?", (row["candidate_id"],)).fetchone()
            if candidate: self._promote_waiting(conn, candidate["hospital"])
            Repository.audit(conn, allocation_id, donor["id"], actor, role, "allocation_expired", {"reason": "organ_window_elapsed"})
            raise ApiError(409, "organ_expired", "器官已经超过可用时间，禁止继续流转")
        return row

    def accept(self, allocation_id: int, actor: str, role: str, hospital: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "hospital": raise ApiError(403, "hospital_required", "只有接收医院可以接受器官")
        expected = body.get("expected_revision")
        if not isinstance(expected, int): raise ApiError(400, "revision_required", "expected_revision 必填")
        with self.repo.tx() as conn:
            row = self._ensure_active(conn, allocation_id, actor, role)
            candidate = conn.execute("SELECT * FROM candidates WHERE id=?", (row["candidate_id"],)).fetchone()
            if candidate["hospital"] != hospital: raise ApiError(403, "wrong_hospital", "只能由候选患者所在医院接受")
            if row["status"] == "accepted": return self._allocation(conn, allocation_id, role, hospital)
            if row["status"] != "proposed": raise ApiError(409, "invalid_transition", "当前状态不能接受")
            if row["revision"] != expected: raise ApiError(409, "revision_conflict", "分配信息已发生变化")
            conn.execute("UPDATE allocations SET status='accepted',accepted_at=?,revision=revision+1,updated_at=? WHERE id=?", (iso(), iso(), allocation_id))
            Repository.audit(conn, allocation_id, row["donor_id"], actor, role, "allocation_accepted", {"hospital": hospital})
            return self._allocation(conn, allocation_id, role, hospital)

    def mark_transit(self, allocation_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "allocation_officer": raise ApiError(403, "transit_forbidden", "只有分配员可以登记转运")
        temp = body.get("cold_chain_temp")
        if not isinstance(temp, (int, float)) or not -2 <= float(temp) <= 8:
            raise ApiError(409, "cold_chain_violation", "冷链温度必须保持在 -2°C 到 8°C")
        with self.repo.tx() as conn:
            row = self._ensure_active(conn, allocation_id, actor, role)
            if row["status"] != "accepted": raise ApiError(409, "invalid_transition", "只有已接受分配可以进入转运")
            conn.execute("UPDATE allocations SET status='in_transit',cold_chain_temp=?,revision=revision+1,updated_at=? WHERE id=?", (float(temp), iso(), allocation_id))
            Repository.audit(conn, allocation_id, row["donor_id"], actor, role, "transfer_started", {"cold_chain_temp": temp})
            return self._allocation(conn, allocation_id, role, "")

    def report_delay(self, allocation_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"allocation_officer", "hospital"}: raise ApiError(403, "delay_forbidden", "当前角色不能上报延误")
        minutes, reason = body.get("delayed_minutes"), str(body.get("reason", "")).strip()
        if not isinstance(minutes, int) or minutes <= 0 or not reason: raise ApiError(400, "invalid_delay", "delayed_minutes 必须为正整数且 reason 必填")
        with self.repo.tx() as conn:
            row = self._ensure_active(conn, allocation_id, actor, role)
            conn.execute("UPDATE allocations SET delayed_minutes=delayed_minutes+?,revision=revision+1,updated_at=? WHERE id=?", (minutes, iso(), allocation_id))
            Repository.audit(conn, allocation_id, row["donor_id"], actor, role, "logistics_delay", {"minutes": minutes, "reason": reason, "at_risk": minutes >= 120})
            return self._allocation(conn, allocation_id, role, "")

    def initiate_handoff(self, allocation_id: int, actor: str, role: str, hospital: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "hospital": raise ApiError(403, "handoff_forbidden", "只有医院可以发起交接")
        expected = body.get("expected_revision"); target = str(body.get("to_hospital", "")).strip(); temp = body.get("cold_chain_temp")
        if not isinstance(expected, int) or not target or not isinstance(temp, (int, float)) or not -2 <= float(temp) <= 8:
            raise ApiError(400, "invalid_handoff", "expected_revision、to_hospital 和合规冷链温度必填")
        with self.repo.tx() as conn:
            row = self._ensure_active(conn, allocation_id, actor, role)
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
        with self.repo.tx() as conn:
            row = self._ensure_active(conn, allocation_id, actor, role)
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
        with self.repo.tx() as conn:
            row = self._ensure_active(conn, allocation_id, actor, role)
            if row["status"] != "handed_off": raise ApiError(409, "invalid_transition", "交接完成后才能确认植入")
            conn.execute("UPDATE allocations SET status='implanted',implanted_at=?,revision=revision+1,updated_at=? WHERE id=?", (iso(), iso(), allocation_id))
            conn.execute("UPDATE donors SET status='used',revision=revision+1 WHERE id=?", (row["donor_id"],))
            Repository.audit(conn, allocation_id, row["donor_id"], actor, role, "organ_implanted", {"candidate_id": row["candidate_id"]})
            return self._allocation(conn, allocation_id, role, "")

    def withdraw(self, allocation_id: int, actor: str, role: str, hospital: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "hospital": raise ApiError(403, "withdraw_forbidden", "只有医院可以撤回")
        reason = str(body.get("reason", "")).strip()
        if not reason: raise ApiError(400, "reason_required", "撤回原因必填")
        with self.repo.tx() as conn:
            row = self._ensure_active(conn, allocation_id, actor, role)
            candidate = conn.execute("SELECT * FROM candidates WHERE id=?", (row["candidate_id"],)).fetchone()
            if hospital != candidate["hospital"]: raise ApiError(403, "wrong_hospital", "只能由候选患者医院撤回")
            if row["status"] not in {"proposed", "accepted"}: raise ApiError(409, "invalid_transition", "转运开始后不能直接撤回")
            conn.execute("UPDATE allocations SET status='withdrawn',revision=revision+1,updated_at=? WHERE id=?", (iso(), allocation_id))
            donor = conn.execute("SELECT * FROM donors WHERE id=?", (row["donor_id"],)).fetchone()
            donor_status = "available" if parse_time(donor["expires_at"]) > utcnow() else "expired"
            conn.execute("UPDATE donors SET status=?,revision=revision+1 WHERE id=?", (donor_status, row["donor_id"]))
            self._release_hold(conn, allocation_id)
            candidate = conn.execute("SELECT hospital FROM candidates WHERE id=?", (row["candidate_id"],)).fetchone()
            if candidate: self._promote_waiting(conn, candidate["hospital"])
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
        return {"donors": donors, "candidates": candidates, "allocations": allocated, "server_time": iso()}


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
        if path == "/api/or-board": return 200, self.service.or_board(role, hospital)
        parts = [p for p in path.split("/") if p]
        if len(parts) == 4 and parts[:2] == ["api", "donors"] and parts[2].isdigit() and parts[3] == "ranking": return 200, self.service.ranking(int(parts[2]), role, hospital)
        if len(parts) == 3 and parts[:2] == ["api", "allocations"] and parts[2].isdigit(): return 200, self.service.get_allocation(int(parts[2]), role, hospital)
        if len(parts) == 4 and parts[:2] == ["api", "allocations"] and parts[2].isdigit() and parts[3] == "audit": return 200, {"audit": self.service.audit(int(parts[2]), role)}
        raise ApiError(404, "not_found", "接口不存在")
    def dispatch_post(self, path: str) -> tuple[int, Any]:
        actor, role, hospital = self.service.identity(self.headers); body = self.read_body(); parts = [p for p in path.split("/") if p]
        if path == "/api/or-slots": return 201, self.service.register_slot(actor, role, hospital, body)
        if path == "/api/allocations":
            payload = self.service.propose(actor, role, body)
            return (202 if payload.pop("_queued", False) else 201, payload)
        actions = {
            "/api/donors": lambda: (201, self.service.register_donor(actor, role, body)),
            "/api/candidates": lambda: (201, self.service.register_candidate(actor, role, body)),
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
        except ApiError as exc: json_reply(self, exc.status, {"error": exc.code, "message": exc.message})
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
