import sys, tempfile, unittest, threading
from datetime import timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import ApiError, OrganAllocationService, iso, utcnow


class OrganFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.svc = OrganAllocationService(Path(self.tmp.name) / "test.db"); self.now = utcnow()

    def tearDown(self): self.tmp.cleanup()

    def donor(self, expires_days=2):
        return self.svc.register_donor("coord", "coordinator", {"blood_type": "O", "organ": "kidney", "hospital": "H1", "region": "East", "available_at": iso(self.now - timedelta(days=3)), "expires_at": iso(self.now + timedelta(days=expires_days)), "clinical_match": 8})

    def candidate(self, name="患者甲", hospital="H2", urgency=5, wait=500):
        return self.svc.register_candidate("coord", "coordinator", {"patient_name": name, "blood_type": "B", "organ": "kidney", "hospital": hospital, "region": "East", "urgency": urgency, "wait_days": wait, "willing": True, "clinical_match": 9})

    def test_complete_allocation_and_cold_chain_flow(self):
        donor, candidate = self.donor(), self.candidate()
        rank = self.svc.ranking(donor["id"], "allocation_officer", "")
        self.assertEqual(rank["candidates"][0]["id"], candidate["id"])
        allocation = self.svc.propose("allocator", "allocation_officer", {"donor_id": donor["id"], "candidate_id": candidate["id"]})
        accepted = self.svc.accept(allocation["id"], "hospital-h2", "hospital", "H2", {"expected_revision": 1})
        self.assertEqual(accepted["status"], "accepted")
        transit = self.svc.mark_transit(allocation["id"], "allocator", "allocation_officer", {"cold_chain_temp": 3.5})
        self.assertEqual(transit["status"], "in_transit")
        handoff = self.svc.initiate_handoff(allocation["id"], "hospital-h1", "hospital", "H1", {"expected_revision": transit["revision"], "to_hospital": "H2", "cold_chain_temp": 3.0})
        self.assertEqual(handoff["handoff"]["status"], "initiated")
        received = self.svc.accept_handoff(allocation["id"], "hospital-h2", "hospital", "H2", {})
        self.assertEqual(received["status"], "handed_off")
        implanted = self.svc.implant(allocation["id"], "allocator", "allocation_officer", {})
        self.assertEqual(implanted["status"], "implanted")
        audit = self.svc.audit(allocation["id"], "auditor")
        self.assertEqual([item["action"] for item in audit], ["allocation_proposed", "allocation_accepted", "transfer_started", "handoff_initiated", "handoff_accepted", "organ_implanted"])

    def test_expiry_privacy_and_single_allocation(self):
        expired = self.donor(expires_days=-1); candidate = self.candidate()
        with self.assertRaises(ApiError) as ctx:
            self.svc.propose("allocator", "allocation_officer", {"donor_id": expired["id"], "candidate_id": candidate["id"]})
        self.assertEqual(ctx.exception.code, "organ_expired")
        donor2 = self.donor(); allocation = self.svc.propose("allocator", "allocation_officer", {"donor_id": donor2["id"], "candidate_id": candidate["id"]})
        with self.assertRaises(ApiError) as ctx:
            self.svc.accept(allocation["id"], "wrong", "hospital", "H1", {"expected_revision": 1})
        self.assertEqual(ctx.exception.status, 403)
        masked = self.svc.get_allocation(allocation["id"], "hospital", "H1")
        self.assertEqual(masked["patient_name"], "***")
        with self.assertRaises(ApiError) as ctx:
            self.svc.propose("allocator", "allocation_officer", {"donor_id": donor2["id"], "candidate_id": candidate["id"]})
        self.assertEqual(ctx.exception.code, "donor_unavailable")
        other = self.candidate("患者乙", "H2", 4, 300)
        self.assertNotEqual(other["id"], candidate["id"])
        with self.assertRaises(ApiError) as ctx:
            self.svc.mark_transit(allocation["id"], "allocator", "allocation_officer", {"cold_chain_temp": 12})
        self.assertEqual(ctx.exception.code, "cold_chain_violation")


class ORSchedulingTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.svc = OrganAllocationService(Path(self.tmp.name) / "or.db"); self.now = utcnow()

    def tearDown(self): self.tmp.cleanup()

    def donor(self, hospital="H1", days=2):
        return self.svc.register_donor("coord", "coordinator", {"blood_type": "O", "organ": "kidney", "hospital": hospital, "region": "East",
             "available_at": iso(self.now - timedelta(days=3)), "expires_at": iso(self.now + timedelta(days=days)), "clinical_match": 8})

    def candidate(self, name, hospital="H2", urgency=5, wait=500):
        return self.svc.register_candidate("coord", "coordinator", {"patient_name": name, "blood_type": "B", "organ": "kidney",
             "hospital": hospital, "region": "East", "urgency": urgency, "wait_days": wait, "willing": True, "clinical_match": 9})

    def slot(self, hospital="H2", capacity=1, date=None):
        return self.svc.register_slot("coord", "coordinator", "", {"hospital": hospital, "slot_date": date or iso(self.now)[:10], "period": "DAY", "capacity": capacity})

    def allocate(self, name, token=None):
        d, c = self.donor(), self.candidate(name)
        body = {"donor_id": d["id"], "candidate_id": c["id"]}
        if token: body["client_token"] = token
        return self.svc.propose("allocator", "allocation_officer", body)

    def test_no_slots_registered_means_unlimited(self):
        result = self.allocate("甲")
        self.assertEqual(result["status"], "proposed")
        self.assertIsNone(result["slot"])

    def test_propose_occupies_slot_and_board_shows_remaining(self):
        self.slot(capacity=1)
        result = self.allocate("甲")
        self.assertEqual(result["status"], "proposed")
        self.assertEqual(result["remaining"], 0)
        self.assertEqual(result["slot"]["held"], 1)
        board = self.svc.or_board("coordinator", "")
        self.assertEqual(board["slots"][0]["held"], 1)
        self.assertEqual(board["slots"][0]["remaining"], 0)
        self.assertEqual(len(board["slots"][0]["holds"]), 1)

    def test_full_capacity_queues_and_reports_remaining(self):
        self.slot(capacity=1)
        first = self.allocate("甲")
        self.assertEqual(first["status"], "proposed")
        second = self.allocate("乙")
        self.assertEqual(second["status"], "queued")
        self.assertEqual(second["remaining"], 0)
        self.assertEqual(second["queue_position"], 1)
        board = self.svc.or_board("coordinator", "")
        self.assertEqual(board["slots"][0]["held"], 1)
        self.assertEqual(board["slots"][0]["remaining"], 0)
        self.assertEqual(len(board["queue"]), 1)
        self.assertEqual(board["queue"][0]["allocation_id"], second["id"])

    def test_withdraw_releases_slot_and_promotes_queue(self):
        self.slot(capacity=1)
        first = self.allocate("甲")
        second = self.allocate("乙")
        self.assertEqual(second["status"], "queued")
        self.svc.withdraw(first["id"], "h2", "hospital", "H2", {"reason": "撤回"})
        promoted = self.svc.get_allocation(second["id"], "allocation_officer", "")
        self.assertEqual(promoted["status"], "proposed")
        self.assertEqual(promoted["slot"]["hold_status"], "held")
        board = self.svc.or_board("coordinator", "")
        self.assertEqual(board["slots"][0]["held"], 1)
        self.assertEqual(len(board["queue"]), 0)

    def test_idempotent_retry_does_not_double_occupy(self):
        self.slot(capacity=2)
        first = self.allocate("甲", token="tok-1")
        retry = self.allocate("甲", token="tok-1")
        self.assertEqual(first["id"], retry["id"])
        self.assertTrue(retry.get("replayed"))
        self.assertEqual(self.svc.repo.conn.execute("SELECT COUNT(*) c FROM or_holds WHERE client_token='tok-1'").fetchone()["c"], 1)
        self.assertEqual(self.svc.repo.conn.execute("SELECT COUNT(*) c FROM allocations WHERE client_token='tok-1'").fetchone()["c"], 1)
        board = self.svc.or_board("coordinator", "")
        self.assertEqual(board["slots"][0]["held"], 1)

    def test_idempotent_retry_of_queued_allocation(self):
        self.slot(capacity=1)
        self.allocate("占位")
        first = self.allocate("排队", token="tok-q")
        self.assertEqual(first["status"], "queued")
        retry = self.allocate("排队", token="tok-q")
        self.assertEqual(first["id"], retry["id"])
        self.assertTrue(retry.get("replayed"))
        self.assertEqual(retry["status"], "queued")

    def test_expiry_releases_slot_and_promotes_queue(self):
        self.slot(capacity=1)
        first = self.allocate("甲")
        second = self.allocate("乙")
        self.assertEqual(second["status"], "queued")
        self.svc.repo.conn.execute("UPDATE donors SET expires_at=? WHERE id=?", (iso(self.now - timedelta(hours=1)), first["donor_id"]))
        board = self.svc.or_board("coordinator", "")
        expired = self.svc.get_allocation(first["id"], "allocation_officer", "")
        promoted = self.svc.get_allocation(second["id"], "allocation_officer", "")
        self.assertEqual(expired["status"], "expired")
        self.assertEqual(promoted["status"], "proposed")
        self.assertEqual(promoted["slot"]["hold_status"], "held")
        self.assertEqual(board["slots"][0]["held"], 1)
        self.assertEqual(len(board["queue"]), 0)

    def test_board_scoped_to_hospital(self):
        self.slot(hospital="H2", capacity=1)
        self.slot(hospital="H3", capacity=1)
        board = self.svc.or_board("hospital", "H2")
        self.assertEqual([s["hospital"] for s in board["slots"]], ["H2"])

    def test_concurrent_proposals_only_one_takes_last_slot(self):
        self.slot(capacity=1)
        results = []
        barrier = threading.Barrier(2)
        def worker(i):
            d, c = self.donor(), self.candidate(f"并发{i}")
            barrier.wait()
            results.append(self.svc.propose("allocator", "allocation_officer",
                                            {"donor_id": d["id"], "candidate_id": c["id"], "client_token": f"race-{i}"}))
        threads = [threading.Thread(target=worker, args=(i,)) for i in range(2)]
        for t in threads: t.start()
        for t in threads: t.join()
        statuses = sorted(r["status"] for r in results)
        self.assertEqual(statuses, ["proposed", "queued"])
        board = self.svc.or_board("coordinator", "")
        self.assertEqual(board["slots"][0]["held"], 1)
        self.assertEqual(board["slots"][0]["remaining"], 0)
        self.assertEqual(len(board["queue"]), 1)


if __name__ == "__main__": unittest.main()
