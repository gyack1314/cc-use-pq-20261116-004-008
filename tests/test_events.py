from __future__ import annotations

from orbitbridge import catalog, events, ingest, quality, reconcile, store
from helpers import DatabaseCase, STAMP, observation, packet


def obs(identifier: str, *, region: str = "north", **kwargs):
    return observation(identifier, region=region, **kwargs)


class EventIngestTests(DatabaseCase):
    def test_each_observation_starts_in_its_own_active_event(self):
        ingest.ingest_bytes(self.db, packet(1, [obs("sim-1")]), when=STAMP)
        ingest.ingest_bytes(self.db, packet(2, [obs("sim-2")]), when=STAMP)
        current = events.list_events(self.db)
        self.assertEqual(len(current), 2)
        self.assertEqual({tuple(e["observation_ids"]) for e in current}, {(1,), (2,)})
        detail = events.event_detail(self.db, 1)
        self.assertEqual([m["change_type"] for m in detail["memberships"]], ["joined"])
        self.assertEqual([line["operation"] for line in detail["lineage"]], ["created"])

    def test_replay_returns_to_same_current_event_without_rebuilding(self):
        ingest.ingest_bytes(self.db, packet(1, [obs("sim-1")]), when=STAMP)
        # 先制造一次真实聚合
        ingest.ingest_bytes(self.db, packet(2, [obs("sim-2")]), when=STAMP)
        events.merge_events(self.db, [1, 2], "值班长", "同一火点", when=STAMP)
        events_before = store.counts(self.db)["events"]
        memberships_before = store.counts(self.db)["event_memberships"]
        # 原始包重放：duplicate，事件与成员关系不新增
        result = ingest.ingest_bytes(self.db, packet(1, [obs("sim-1")]), when=STAMP)
        self.assertEqual(result.status, "duplicate")
        self.assertEqual(store.counts(self.db)["events"], events_before)
        self.assertEqual(store.counts(self.db)["event_memberships"], memberships_before)
        self.assertEqual(events.current_event_id(self.db, 1), 1)
        self.assertEqual(events.current_event_id(self.db, 2), 1)

    def test_observation_without_subscription_still_gets_event(self):
        result = ingest.ingest_bytes(self.db, packet(1, [obs("sim-1", region="south")]), when=STAMP)
        self.assertEqual(result.handoff_count, 0)
        self.assertEqual(len(events.list_events(self.db)), 1)


class MergeTests(DatabaseCase):
    def _two(self):
        ingest.ingest_bytes(self.db, packet(1, [obs("sim-1")]), when=STAMP)
        ingest.ingest_bytes(self.db, packet(2, [obs("sim-2")]), when=STAMP)

    def test_confirmed_merge_keeps_packets_and_moves_membership_with_lineage(self):
        self._two()
        proposal = events.propose_merge(self.db, [1, 2], "值班长", "同一火点", when=STAMP)
        self.assertEqual(proposal["status"], "proposed")
        confirmed = events.confirm_proposal(self.db, proposal["proposal_id"], "值班长", when=STAMP)
        self.assertEqual(confirmed["status"], "confirmed")
        self.assertEqual(confirmed["resulting_event_ids"], [1])
        target = events.event_detail(self.db, 1)
        self.assertEqual(target["observation_ids"], [1, 2])
        self.assertEqual(events.get_event(self.db, 2)["status"], "absorbed")
        # 原始观测与交接项一条不少
        self.assertEqual(store.counts(self.db)["observations"], 2)
        self.assertEqual(store.counts(self.db)["handoffs"], 2)
        operations = [line["operation"] for line in target["lineage"]]
        self.assertIn("merged", operations)
        # 观测 2 的成员轨迹：joined(事件2) -> left -> joined(事件1)
        trail = events.audit_observation(self.db, 2)["memberships"]
        self.assertEqual([m["change_type"] for m in trail], ["joined", "left", "joined"])

    def test_merge_arranges_one_duty_listing_only_entitled_sources(self):
        self._two()
        events.merge_events(self.db, [1, 2], "值班长", "同一火点", when=STAMP)
        pending = events.list_duties(self.db, subscription_id="north-watch", status="pending")
        self.assertEqual(len(pending), 1)
        snapshot = events.duty_detail(self.db, pending[0]["duty_id"])["snapshot"]
        self.assertEqual({row["observation_id"] for row in snapshot}, {1, 2})

    def test_cross_region_merge_never_exposes_unauthorized_sources(self):
        catalog.register_subscription(self.db, "south-watch", "south", 1, "0", when=STAMP)
        ingest.ingest_bytes(self.db, packet(1, [obs("n-1", region="north")]), when=STAMP)
        ingest.ingest_bytes(self.db, packet(2, [obs("s-1", region="south")]), when=STAMP)
        events.merge_events(self.db, [1, 2], "值班长", "跨区合并", when=STAMP)
        north = events.list_duties(self.db, subscription_id="north-watch", status="pending")
        south = events.list_duties(self.db, subscription_id="south-watch", status="pending")
        north_sources = {r["external_id"] for d in north
                         for r in events.duty_detail(self.db, d["duty_id"])["snapshot"]}
        south_sources = {r["external_id"] for d in south
                         for r in events.duty_detail(self.db, d["duty_id"])["snapshot"]}
        self.assertEqual(north_sources, {"n-1"})
        self.assertEqual(south_sources, {"s-1"})

    def test_merge_of_two_acknowledged_events_preserves_both_histories(self):
        catalog.register_subscription(self.db, "south-watch", "south", 1, "0", when=STAMP)
        ingest.ingest_bytes(self.db, packet(1, [obs("n-1", region="north")]), when=STAMP)
        ingest.ingest_bytes(self.db, packet(2, [obs("s-1", region="south")]), when=STAMP)
        for subscription in ("north-watch", "south-watch"):
            duty = events.list_duties(self.db, subscription_id=subscription, status="pending")[0]
            events.acknowledge_duty(self.db, duty["duty_id"], f"员-{subscription}", when=STAMP)
        events.merge_events(self.db, [1, 2], "值班长", "已确认后合并", when=STAMP)
        # 无新待办，两条 acknowledged 代际连同各自来源快照保留
        self.assertEqual(events.list_duties(self.db, status="pending"), [])
        history = events.list_duties(self.db, status="acknowledged")
        self.assertEqual(len(history), 2)
        operators = {d["operator"] for d in history}
        self.assertEqual(operators, {"员-north-watch", "员-south-watch"})

    def test_merge_requires_two_distinct_active_events(self):
        ingest.ingest_bytes(self.db, packet(1, [obs("sim-1")]), when=STAMP)
        with self.assertRaises(events.EventError) as error:
            events.merge_events(self.db, [1], "值班长", "r")
        self.assertEqual(error.exception.code, "merge_targets")
        with self.assertRaises(events.EventError) as error:
            events.propose_merge(self.db, [1, 99], "值班长", "r")
        self.assertEqual(error.exception.code, "event_missing")

    def test_decided_proposal_cannot_be_confirmed_twice(self):
        self._two()
        proposal = events.propose_merge(self.db, [1, 2], "值班长", "r", when=STAMP)
        events.confirm_proposal(self.db, proposal["proposal_id"], "值班长", when=STAMP)
        with self.assertRaises(events.EventError) as error:
            events.confirm_proposal(self.db, proposal["proposal_id"], "值班长", when=STAMP)
        self.assertEqual(error.exception.code, "proposal_decided")

    def test_proposal_can_be_cancelled_without_touching_events(self):
        self._two()
        proposal = events.propose_merge(self.db, [1, 2], "值班长", "r", when=STAMP)
        events.cancel_proposal(self.db, proposal["proposal_id"], "值班长", when=STAMP)
        self.assertEqual(events.get_proposal(self.db, proposal["proposal_id"])["status"], "cancelled")
        self.assertEqual(len(events.list_events(self.db, status="active")), 2)


class SplitTests(DatabaseCase):
    def test_confirmed_split_creates_new_events_and_rearranges_duties(self):
        ingest.ingest_bytes(self.db, packet(1, [obs("sim-1"), obs("sim-2")]), when=STAMP)
        # 两个单例事件先聚合，再拆分
        events.merge_events(self.db, [1, 2], "值班长", "同一火点", when=STAMP)
        self.assertEqual(events.current_event_id(self.db, 1), 1)
        self.assertEqual(events.current_event_id(self.db, 2), 1)
        proposal = events.propose_split(self.db, 1, [[1], [2]], "值班长", "两个火点", when=STAMP)
        result = events.confirm_proposal(self.db, proposal["proposal_id"], "值班长", when=STAMP)
        # 原两个单例事件归并，拆分产生两个新事件（编号在其后）
        new_ids = sorted(result["resulting_event_ids"])
        self.assertEqual(len(new_ids), 2)
        self.assertEqual(min(new_ids), 3)
        # 原事件归并，两个新事件各持一条观测
        self.assertEqual(events.get_event(self.db, 1)["status"], "absorbed")
        groups = {tuple(events.get_event(self.db, eid)["observation_ids"])
                  for eid in result["resulting_event_ids"]}
        self.assertEqual(groups, {(1,), (2,)})
        # 旧待办作废，新事件各有一条 pending 待办
        self.assertEqual(len(events.list_duties(self.db, status="pending")), 2)
        self.assertTrue(any(d["status"] == "superseded" for d in events.list_duties(self.db)))

    def test_split_groups_must_cover_exact_membership(self):
        ingest.ingest_bytes(self.db, packet(1, [obs("sim-1"), obs("sim-2")]), when=STAMP)
        with self.assertRaises(events.EventError) as error:
            events.propose_split(self.db, 1, [[1]], "值班长", "r")
        self.assertEqual(error.exception.code, "split_groups")
        with self.assertRaises(events.EventError) as error:
            events.propose_split(self.db, 1, [[1], [1]], "值班长", "r")
        self.assertEqual(error.exception.code, "split_groups")

    def test_split_preserves_acknowledged_generation_history(self):
        ingest.ingest_bytes(self.db, packet(1, [obs("sim-1"), obs("sim-2")]), when=STAMP)
        events.merge_events(self.db, [1, 2], "值班长", "先聚合", when=STAMP)
        duty = events.list_duties(self.db, status="pending")[0]
        events.acknowledge_duty(self.db, duty["duty_id"], "值守员甲", when=STAMP)
        # 再来一条并入，产生新一代待办后拆分
        ingest.ingest_bytes(self.db, packet(2, [obs("sim-3")]), when=STAMP)
        events.merge_events(self.db, [1, 3], "值班长", "并入", when=STAMP)
        events.propose_split(self.db, 1, [[1, 2], [3]], "值班长", "拆", when=STAMP)
        proposal = events.list_proposals(self.db, status="proposed")[0]
        events.confirm_proposal(self.db, proposal["proposal_id"], "值班长", when=STAMP)
        acknowledged = events.list_duties(self.db, status="acknowledged")
        self.assertEqual(len(acknowledged), 1)
        self.assertEqual(acknowledged[0]["operator"], "值守员甲")


class DutyTests(DatabaseCase):
    def test_acknowledgement_only_covers_sources_listed_at_the_time(self):
        ingest.ingest_bytes(self.db, packet(1, [obs("sim-1"), obs("sim-2")]), when=STAMP)
        events.merge_events(self.db, [1, 2], "值班长", "聚合", when=STAMP)
        duty = events.list_duties(self.db, event_id=1, status="pending")[0]
        first_generation = duty["generation"]
        events.acknowledge_duty(self.db, duty["duty_id"], "值守员甲", when=STAMP)
        # 后来加入的观测产生新一代 pending，旧确认不自动覆盖
        ingest.ingest_bytes(self.db, packet(2, [obs("sim-3")]), when=STAMP)
        events.merge_events(self.db, [1, 3], "值班长", "后续并入", when=STAMP)
        new_duty = events.list_duties(self.db, event_id=1, status="pending")[0]
        self.assertGreater(new_duty["generation"], first_generation)
        snapshot = events.duty_detail(self.db, new_duty["duty_id"])["snapshot"]
        pending_rows = [row for row in snapshot if row["was_pending"] == 1]
        self.assertEqual([row["external_id"] for row in pending_rows], ["sim-3"])
        # 底层义务：前两条已确认，第三条仍 pending
        statuses = {row["external_id"]: row["handoff_status"] for row in snapshot}
        self.assertEqual(statuses["sim-1"], "acknowledged")
        self.assertEqual(statuses["sim-3"], "pending")

    def test_ack_duty_marks_underlying_handoffs(self):
        ingest.ingest_bytes(self.db, packet(1, [obs("sim-1")]), when=STAMP)
        duty = events.list_duties(self.db, status="pending")[0]
        events.acknowledge_duty(self.db, duty["duty_id"], "值守员甲", when=STAMP)
        handoff = store.one(self.db, "SELECT * FROM handoffs WHERE observation_id=1")
        self.assertEqual(handoff["status"], "acknowledged")
        self.assertEqual(handoff["operator"], "值守员甲")

    def test_legacy_per_handoff_ack_reconciles_containing_duty(self):
        from orbitbridge import handoff as handoff_module
        ingest.ingest_bytes(self.db, packet(1, [obs("sim-1")]), when=STAMP)
        ingest.ingest_bytes(self.db, packet(2, [obs("sim-2")]), when=STAMP)
        events.merge_events(self.db, [1, 2], "值班长", "聚合", when=STAMP)
        for item in handoff_module.list_handoffs(self.db):
            handoff_module.acknowledge(self.db, item["handoff_id"], "值守员乙", when=STAMP)
        # 底层义务全部确认后，包含它们的待办也被收敛为 acknowledged，不残留悬空 pending 待办
        self.assertEqual(store.integrity_check(self.db), [])
        current = [d for d in events.list_duties(self.db) if d["status"] == "acknowledged"]
        self.assertTrue(any(d["operator"] == "值守员乙" for d in current))

    def test_superseded_duty_cannot_be_acknowledged(self):
        ingest.ingest_bytes(self.db, packet(1, [obs("sim-1")]), when=STAMP)
        duty = events.list_duties(self.db, status="pending")[0]
        ingest.ingest_bytes(self.db, packet(2, [obs("sim-2")]), when=STAMP)
        events.merge_events(self.db, [1, 2], "值班长", "r", when=STAMP)
        with self.assertRaises(events.EventError) as error:
            events.acknowledge_duty(self.db, duty["duty_id"], "值守员甲")
        self.assertEqual(error.exception.code, "duty_not_pending")


class CorrectionTests(DatabaseCase):
    def test_region_correction_withdraws_old_obligation_and_creates_new(self):
        catalog.register_subscription(self.db, "south-watch", "south", 1, "0", when=STAMP)
        ingest.ingest_bytes(self.db, packet(1, [obs("sim-1", region="north")]), when=STAMP)
        result = events.correct_observation(self.db, 1, {"region": "south"}, "制图员", "区域修正", when=STAMP)
        by_sub = {row["subscription_id"]: row["status"] for row in result["handoffs"]}
        self.assertEqual(by_sub["north-watch"], "withdrawn")
        self.assertEqual(by_sub["south-watch"], "pending")
        # 撤销是留痕而非删除
        self.assertEqual(store.counts(self.db)["handoffs"], 2)
        self.assertEqual(len(result["corrections"]), 1)
        self.assertEqual(result["corrections"][0]["old_value"], "north")
        # 待办按新覆盖重排：south 有 pending，north 无
        self.assertEqual(len(events.list_duties(self.db, subscription_id="south-watch", status="pending")), 1)
        self.assertEqual(events.list_duties(self.db, subscription_id="north-watch", status="pending"), [])

    def test_coordinate_correction_is_recorded_without_dropping_data(self):
        ingest.ingest_bytes(self.db, packet(1, [obs("sim-1")]), when=STAMP)
        result = events.correct_observation(self.db, 1, {"latitude": "31.50000"}, "制图员", "定位修正", when=STAMP)
        self.assertEqual(result["observation"]["latitude"], "31.50000")
        self.assertEqual(result["corrections"][0]["field_name"], "latitude")
        self.assertEqual(store.counts(self.db)["observations"], 1)

    def test_withdraw_then_restore_reuses_obligation_row_with_history(self):
        from orbitbridge import audit
        catalog.register_subscription(self.db, "south-watch", "south", 1, "0", when=STAMP)
        ingest.ingest_bytes(self.db, packet(1, [obs("sim-1", region="north")]), when=STAMP)
        north_handoff = store.one(self.db,
                                  "SELECT * FROM handoffs WHERE observation_id=1 AND subscription_id='north-watch'")
        events.correct_observation(self.db, 1, {"region": "south"}, "制图员", "r1", when=STAMP)
        withdrawn = store.one(self.db, "SELECT * FROM handoffs WHERE handoff_id=?", (north_handoff["handoff_id"],))
        self.assertEqual(withdrawn["status"], "withdrawn")
        events.correct_observation(self.db, 1, {"region": "north"}, "制图员", "r2", when=STAMP)
        restored = store.one(self.db, "SELECT * FROM handoffs WHERE handoff_id=?", (north_handoff["handoff_id"],))
        self.assertEqual(restored["status"], "pending")
        same_pair = store.one(
            self.db,
            "SELECT COUNT(*) AS c FROM handoffs WHERE observation_id=1 AND subscription_id='north-watch'")
        self.assertEqual(same_pair["c"], 1)
        kinds = [row["event_type"] for row in audit.list_activity(
            self.db, entity_type="handoff", entity_id=str(north_handoff["handoff_id"]))]
        self.assertIn("handoff_withdrawn", kinds)
        self.assertIn("handoff_restored", kinds)

    def test_invalid_correction_is_rejected(self):
        ingest.ingest_bytes(self.db, packet(1, [obs("sim-1")]), when=STAMP)
        with self.assertRaises(events.EventError):
            events.correct_observation(self.db, 1, {"latitude": 999}, "制图员", "r")
        with self.assertRaises(events.EventError) as error:
            events.correct_observation(self.db, 1, {}, "制图员", "r")
        self.assertEqual(error.exception.code, "changes")


class SubscriptionCoverageTests(DatabaseCase):
    def test_deactivation_keeps_obligations_and_reactivation_rearranges(self):
        ingest.ingest_bytes(self.db, packet(1, [obs("sim-1")]), when=STAMP)
        catalog.set_subscription_active(self.db, "north-watch", False)
        # 义务不消失
        self.assertEqual(store.one(self.db, "SELECT COUNT(*) AS c FROM handoffs")["c"], 1)
        catalog.set_subscription_active(self.db, "north-watch", True)
        pending = events.list_duties(self.db, subscription_id="north-watch", status="pending")
        self.assertEqual(len(pending), 1)

    def test_reactivation_does_not_backfill_historical_observations(self):
        catalog.set_subscription_active(self.db, "north-watch", False)
        ingest.ingest_bytes(self.db, packet(1, [obs("sim-1")]), when=STAMP)
        self.assertEqual(store.one(self.db, "SELECT COUNT(*) AS c FROM handoffs")["c"], 0)
        catalog.set_subscription_active(self.db, "north-watch", True)
        self.assertEqual(store.one(self.db, "SELECT COUNT(*) AS c FROM handoffs")["c"], 0)
        self.assertEqual(events.list_duties(self.db, status="pending"), [])


class OverviewTests(DatabaseCase):
    def test_overview_separates_raw_events_and_unfulfilled_obligations(self):
        catalog.register_subscription(self.db, "south-watch", "south", 1, "0", when=STAMP)
        ingest.ingest_bytes(self.db, packet(1, [obs("n-1", region="north"), obs("s-1", region="south")]), when=STAMP)
        overview = events.event_overview(self.db)
        self.assertEqual(overview["raw_observations"], 2)
        self.assertEqual(overview["active_events"], 2)
        self.assertEqual(overview["pending_handoffs"], 2)
        groups = {row["subscription_id"]: row["pending_obligations"] for row in overview["subscriptions"]}
        self.assertEqual(groups["north-watch"], 1)
        self.assertEqual(groups["south-watch"], 1)

    def test_pending_duty_count_is_not_inflated_by_snapshot_join(self):
        catalog.register_subscription(self.db, "north-backup", "north", 1, "0", when=STAMP)
        ingest.ingest_bytes(self.db, packet(1, [obs("sim-1"), obs("sim-2")]), when=STAMP)
        events.merge_events(self.db, [1, 2], "值班长", "聚合", when=STAMP)
        overview = events.event_overview(self.db)
        groups = {row["subscription_id"]: row["pending_duty_count"] for row in overview["subscriptions"]}
        # 每个订阅在合并事件上只有一条 pending 待办，不因快照含两条观测而翻倍
        self.assertEqual(groups["north-watch"], 1)
        self.assertEqual(groups["north-backup"], 1)
        self.assertEqual(overview["pending_duties"], 2)

    def test_withdrawn_records_are_still_counted_not_deleted(self):
        catalog.register_subscription(self.db, "south-watch", "south", 1, "0", when=STAMP)
        ingest.ingest_bytes(self.db, packet(1, [obs("sim-1", region="north")]), when=STAMP)
        events.correct_observation(self.db, 1, {"region": "south"}, "制图员", "r", when=STAMP)
        overview = events.event_overview(self.db)
        self.assertEqual(overview["withdrawn_handoffs"], 1)
        self.assertEqual(overview["total_handoffs"], 2)
        # 事件成员总数仍与原始观测一致
        report = reconcile.event_counts(self.db)
        self.assertTrue(report["balanced"])
        self.assertEqual(report["current_event_members"], 1)

    def test_quality_and_verify_stay_clean_through_full_lifecycle(self):
        catalog.register_subscription(self.db, "south-watch", "south", 1, "0", when=STAMP)
        ingest.ingest_bytes(self.db, packet(1, [obs("n-1", region="north")]), when=STAMP)
        ingest.ingest_bytes(self.db, packet(2, [obs("s-1", region="south")]), when=STAMP)
        events.merge_events(self.db, [1, 2], "值班长", "跨区", when=STAMP)
        events.propose_split(self.db, 1, [[1], [2]], "值班长", "拆", when=STAMP)
        proposal = events.list_proposals(self.db, status="proposed")[0]
        events.confirm_proposal(self.db, proposal["proposal_id"], "值班长", when=STAMP)
        events.correct_observation(self.db, 2, {"region": "north"}, "制图员", "改区", when=STAMP)
        quality_report = quality.check_all(self.db)
        self.assertTrue(quality_report["ok"], quality_report["issues"])
        self.assertEqual(reconcile.full_reconciliation(self.db)["ok"], True)
        from orbitbridge import audit
        self.assertEqual(audit.verify_lineage(self.db), [])
