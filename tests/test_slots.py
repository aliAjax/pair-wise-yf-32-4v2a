import http.client
import json
import sys
import tempfile
import threading
import unittest
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import ApiError, OrganAllocationService, create_server, iso, utcnow


class SlotFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.svc = OrganAllocationService(Path(self.tmp.name) / "test.db")
        self.now = utcnow()
        self.day = self.now.date().isoformat()

    def tearDown(self):
        self.tmp.cleanup()

    def donor(self, index=0, hospital="H1", expires_days=2):
        return self.svc.register_donor("coord", "coordinator", {
            "blood_type": "O", "organ": "kidney", "hospital": hospital, "region": "East",
            "available_at": iso(self.now - timedelta(days=3)),
            "expires_at": iso(self.now + timedelta(days=expires_days, hours=index)),
            "clinical_match": 8})

    def candidate(self, name="患者甲", hospital="H2", urgency=5, wait=500):
        return self.svc.register_candidate("coord", "coordinator", {
            "patient_name": name, "blood_type": "B", "organ": "kidney", "hospital": hospital,
            "region": "East", "urgency": urgency, "wait_days": wait, "clinical_match": 9})

    def register_slot(self, hospital="H2", rooms=1, teams=None, day=None):
        body = {"slot_date": day or self.day, "room_count": rooms}
        if teams is not None: body["anesthesia_team_count"] = teams
        return self.svc.register_slot("hospital-h2", "hospital", hospital, body)

    def propose(self, donor, candidate, token=None, day=None):
        body = {"donor_id": donor["id"], "candidate_id": candidate["id"]}
        if token: body["client_token"] = token
        if day: body["slot_date"] = day
        return self.svc.propose("allocator", "allocation_officer", body)

    def test_register_slot_shows_capacity(self):
        view = self.register_slot(rooms=3, teams=2)
        self.assertEqual(view["capacity"], 2)  # anesthesia teams are the tighter constraint
        self.assertEqual((view["occupied"], view["remaining"], view["queued_count"]), (0, 2, 0))
        view = self.register_slot(rooms=3, teams=2)  # same hospital/day updates, not duplicates
        self.assertEqual(len(self.svc.slots("coordinator", "")["slots"]), 1)
        with self.assertRaises(ApiError) as ctx:
            self.svc.register_slot("h", "hospital", "H2", {"slot_date": self.day, "room_count": 0})
        self.assertEqual(ctx.exception.code, "invalid_room_count")
        with self.assertRaises(ApiError) as ctx:
            self.svc.register_slot("h", "hospital", "H2", {"slot_date": self.day, "room_count": 3, "anesthesia_team_count": 0})
        self.assertEqual(ctx.exception.code, "invalid_anesthesia_team_count")

    def test_propose_occupies_and_overflow_queues(self):
        self.register_slot(rooms=1)
        c = self.candidate()
        a1 = self.propose(self.donor(0), c)
        self.assertEqual(a1["slot"]["status"], "held")
        self.assertEqual(a1["slot"]["remaining"], 0)
        a2 = self.propose(self.donor(1), c)
        self.assertEqual(a2["slot"]["status"], "queued")
        self.assertEqual(a2["slot"]["queue_position"], 1)
        self.assertEqual(a2["slot"]["remaining"], 0)
        view = self.svc.slots("coordinator", "")["slots"][0]
        self.assertEqual((view["occupied"], view["remaining"], view["queued_count"]), (1, 0, 1))
        self.assertEqual(view["queue"][0]["allocation_id"], a2["id"])
        self.assertEqual(view["queue"][0]["patient_name"], "患者甲")

    def test_accept_queued_rejected_until_promoted(self):
        self.register_slot(rooms=1)
        c = self.candidate()
        a1, a2 = self.propose(self.donor(0), c), self.propose(self.donor(1), c)
        with self.assertRaises(ApiError) as ctx:
            self.svc.accept(a2["id"], "hospital-h2", "hospital", "H2", {"expected_revision": 1})
        self.assertEqual(ctx.exception.code, "slot_pending")
        self.assertEqual(ctx.exception.details["remaining"], 0)
        self.assertEqual(ctx.exception.details["queue_position"], 1)
        self.svc.withdraw(a1["id"], "hospital-h2", "hospital", "H2", {"reason": "医院放弃"})
        promoted = self.svc.get_allocation(a2["id"], "allocation_officer", "")
        self.assertEqual(promoted["slot"]["status"], "held")
        accepted = self.svc.accept(a2["id"], "hospital-h2", "hospital", "H2", {"expected_revision": 1})
        self.assertEqual(accepted["status"], "accepted")
        view = self.svc.slots("coordinator", "")["slots"][0]
        self.assertEqual((view["occupied"], view["queued_count"]), (1, 0))

    def test_withdraw_releases_and_fifo_promotion(self):
        self.register_slot(rooms=1)
        c = self.candidate()
        allocations = [self.propose(self.donor(i), c) for i in range(4)]
        self.assertEqual([a["slot"]["status"] for a in allocations], ["held", "queued", "queued", "queued"])
        self.svc.withdraw(allocations[0]["id"], "hospital-h2", "hospital", "H2", {"reason": "放弃"})
        self.assertEqual(self.svc.get_allocation(allocations[1]["id"], "coordinator", "")["slot"]["status"], "held")
        self.svc.withdraw(allocations[1]["id"], "hospital-h2", "hospital", "H2", {"reason": "放弃"})
        self.assertEqual(self.svc.get_allocation(allocations[2]["id"], "coordinator", "")["slot"]["status"], "held")
        self.assertEqual(self.svc.get_allocation(allocations[3]["id"], "coordinator", "")["slot"]["queue_position"], 1)

    def test_expiry_releases_and_promotes(self):
        self.register_slot(rooms=1)
        c = self.candidate()
        a1, a2 = self.propose(self.donor(0, expires_days=1), c), self.propose(self.donor(1, expires_days=2), c)
        # Held organ elapses without anyone touching it; the next flow action performs lazy expiry.
        self.svc.repo.conn.execute("UPDATE donors SET expires_at=? WHERE id=(SELECT donor_id FROM allocations WHERE id=?)",
                                   (iso(self.now - timedelta(minutes=1)), a1["id"]))
        with self.assertRaises(ApiError) as ctx:
            self.svc.mark_transit(a1["id"], "allocator", "allocation_officer", {"cold_chain_temp": 3.0})
        self.assertEqual(ctx.exception.code, "organ_expired")
        promoted = self.svc.get_allocation(a2["id"], "allocation_officer", "")
        self.assertEqual(promoted["slot"]["status"], "held")
        self.assertEqual(promoted["slot"]["remaining"], 0)
        accepted = self.svc.accept(a2["id"], "hospital-h2", "hospital", "H2", {"expected_revision": 1})
        self.assertEqual(accepted["status"], "accepted")

    def test_expired_queued_head_skipped_on_promotion(self):
        self.register_slot(rooms=1)
        c = self.candidate()
        a1 = self.propose(self.donor(0, expires_days=2), c)
        a2 = self.propose(self.donor(1, expires_days=1), c)
        a3 = self.propose(self.donor(2, expires_days=2), c)
        self.svc.repo.conn.execute("UPDATE donors SET expires_at=? WHERE id=(SELECT donor_id FROM allocations WHERE id=?)",
                                   (iso(self.now - timedelta(minutes=1)), a2["id"]))
        self.svc.withdraw(a1["id"], "hospital-h2", "hospital", "H2", {"reason": "放弃"})
        # a2 expired while queued: it is released and a3 skips over it.
        self.assertEqual(self.svc.get_allocation(a3["id"], "coordinator", "")["slot"]["status"], "held")
        view = self.svc.slots("coordinator", "")["slots"][0]
        self.assertEqual((view["occupied"], view["queued_count"]), (1, 0))

    def test_persistence_failure_keeps_hold_and_retry_is_idempotent(self):
        self.register_slot(rooms=1)
        d, c = self.donor(0), self.candidate()
        self.svc.fault_on_next_allocation_insert = True
        with self.assertRaises(ApiError) as ctx:
            self.propose(d, c, token="tok-123")
        self.assertEqual(ctx.exception.status, 500)
        self.assertEqual(ctx.exception.code, "persistence_failed")
        self.assertEqual(ctx.exception.details["slot"]["remaining"], 0)
        hold_id = ctx.exception.details["hold_id"]
        # The failed attempt still occupies the last room.
        view = self.svc.slots("coordinator", "")["slots"][0]
        self.assertEqual(view["occupied"], 1)
        retry = self.propose(d, c, token="tok-123")
        self.assertEqual(retry["slot"]["status"], "held")
        self.assertEqual(retry["hold_id"], hold_id)
        same = self.propose(d, c, token="tok-123")
        self.assertEqual(same["id"], retry["id"])
        rows = self.svc.repo.conn.execute("SELECT COUNT(*) FROM slot_holds").fetchone()[0]
        self.assertEqual(rows, 1)  # retries never create a second hold

    def test_capacity_reduction_below_held_rejected(self):
        self.register_slot(rooms=2)
        c = self.candidate()
        self.propose(self.donor(0), c)
        self.propose(self.donor(1), c)
        with self.assertRaises(ApiError) as ctx:
            self.register_slot(rooms=1)
        self.assertEqual(ctx.exception.code, "capacity_below_held")
        self.assertEqual(ctx.exception.details["occupied"], 2)
        # Equal to or above the occupied count is allowed.
        self.assertEqual(self.register_slot(rooms=2)["remaining"], 0)

    def test_slot_visibility_by_role(self):
        self.register_slot(hospital="H2", rooms=1)
        with self.assertRaises(ApiError) as ctx:
            self.svc.slots("viewer", "")
        self.assertEqual(ctx.exception.code, "slot_forbidden")
        self.svc.register_slot("hospital-h1", "hospital", "H1", {"slot_date": self.day, "room_count": 4})
        h1 = self.svc.slots("hospital", "H1")["slots"]
        self.assertEqual([s["hospital"] for s in h1], ["H1"])
        coord = self.svc.slots("coordinator", "")["slots"]
        self.assertEqual({s["hospital"] for s in coord}, {"H1", "H2"})

    def test_propose_without_registered_slot_keeps_legacy_flow(self):
        d, c = self.donor(0), self.candidate()
        allocation = self.propose(d, c)
        self.assertIsNone(allocation["slot"])
        accepted = self.svc.accept(allocation["id"], "hospital-h2", "hospital", "H2", {"expected_revision": 1})
        self.assertEqual(accepted["status"], "accepted")


class SlotHttpConcurrencyTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "http.db"
        self.server = create_server(self.db, "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.svc = self.server.RequestHandlerClass.service
        self.now = utcnow()
        self.day = self.now.date().isoformat()

    def tearDown(self):
        self.server.shutdown(); self.server.server_close(); self.tmp.cleanup()

    def call(self, method, path, body=None, role="coordinator", hospital=None):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        headers = {"X-User-Id": "tester", "X-Role": role}
        if hospital: headers["X-Hospital"] = hospital
        payload = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
        if payload is not None: headers["Content-Type"] = "application/json"
        conn.request(method, path, body=payload, headers=headers)
        response = conn.getresponse(); raw = response.read().decode()
        conn.close()
        return response.status, json.loads(raw)

    def test_simultaneous_propose_only_first_gets_last_room(self):
        status, _ = self.call("POST", "/api/slots", {"slot_date": self.day, "room_count": 1}, role="hospital", hospital="H2")
        self.assertEqual(status, 201)
        donor_ids, candidate_id = [], None
        for i in range(2):
            _, donor = self.call("POST", "/api/donors", {
                "blood_type": "O", "organ": "kidney", "hospital": "H1", "region": "East",
                "available_at": iso(self.now - timedelta(days=1)),
                "expires_at": iso(self.now + timedelta(days=2))})
            donor_ids.append(donor["id"])
        _, candidate = self.call("POST", "/api/candidates", {
            "patient_name": "并发患者", "blood_type": "B", "organ": "kidney", "hospital": "H2",
            "region": "East", "urgency": 5, "wait_days": 100})
        candidate_id = candidate["id"]

        barrier = threading.Barrier(2)
        results: list[tuple[int, dict]] = []

        def race(donor_id):
            barrier.wait()
            results.append(self.call("POST", "/api/allocations",
                                     {"donor_id": donor_id, "candidate_id": candidate_id, "slot_date": self.day},
                                     role="allocation_officer"))

        threads = [threading.Thread(target=race, args=(donor_id,)) for donor_id in donor_ids]
        for t in threads: t.start()
        for t in threads: t.join()

        statuses = sorted(slot["status"] for _, payload in results for slot in [payload["slot"]])
        self.assertEqual(statuses, ["held", "queued"])
        queued = next(payload for _, payload in results if payload["slot"]["status"] == "queued")
        self.assertEqual(queued["slot"]["remaining"], 0)
        self.assertEqual(queued["slot"]["queue_position"], 1)

        status, payload = self.call("GET", "/api/slots", role="coordinator")
        self.assertEqual(status, 200)
        view = payload["slots"][0]
        self.assertEqual((view["capacity"], view["occupied"], view["remaining"], view["queued_count"]), (1, 1, 0, 1))


if __name__ == "__main__":
    unittest.main()
