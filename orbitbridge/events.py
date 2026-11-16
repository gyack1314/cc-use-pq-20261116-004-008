"""火点事件聚合、拆分、定位修正与事件级值守待办。

设计原则：
- 原始观测（observations）与逐观测接收义务（handoffs）永不删除，只追加或撤销。
- 事件是观测之上的可变聚合视图：成员关系（event_memberships）与血缘（event_lineage）只追加。
- 值守待办按「事件 × 订阅 × 代际」组织；覆盖关系变化时旧待办标记 superseded，按当前覆盖重排。
- 确认只快照当时所列来源（duty_observations），后来加入的观测产生新一代 pending 待办。
"""
from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

from . import catalog, store
from .normalize import REGIONS, iso_utc

SYSTEM_OPERATOR = "system"


class EventError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def _now(when: str | None) -> str:
    return when or catalog.utc_now()


def _operator(operator: str) -> str:
    if not isinstance(operator, str) or not 1 <= len(operator.strip()) <= 100:
        raise EventError("operator", "操作人无效")
    return operator.strip()


# --------------------------------------------------------------------------- 事件与成员


def create_singleton_event(connection, observation_id: int, region: str, when: str) -> int:
    """收件时为每条观测建立独立事件；在 ingest 事务内调用。"""
    cursor = connection.execute(
        "INSERT INTO events(region,status,created_at) VALUES(?,'active',?)", (region, when))
    event_id = int(cursor.lastrowid)
    cursor = connection.execute(
        "INSERT INTO event_memberships(event_id,observation_id,change_type,reason_code,changed_at,operator) "
        "VALUES(?,?,'joined','ingested',?,?)", (event_id, observation_id, when, SYSTEM_OPERATOR))
    membership_id = int(cursor.lastrowid)
    connection.execute(
        "INSERT INTO event_lineage(operation,from_event_id,to_event_id,occurred_at,operator,reason,details_json) "
        "VALUES('created',NULL,?,?,?,?,?)",
        (event_id, when, SYSTEM_OPERATOR, "ingested",
         json.dumps({"observation_id": observation_id, "membership_id": membership_id})))
    return event_id


def current_event_id(connection, observation_id: int) -> int | None:
    row = store.one(
        connection,
        "SELECT event_id FROM event_memberships WHERE observation_id=? "
        "ORDER BY membership_id DESC LIMIT 1", (observation_id,))
    if row is None:
        return None
    latest_change = store.one(
        connection,
        "SELECT change_type FROM event_memberships WHERE observation_id=? ORDER BY membership_id DESC LIMIT 1",
        (observation_id,))
    if latest_change is None or latest_change["change_type"] != "joined":
        return None
    return int(row["event_id"])


def current_member_ids(connection, event_id: int) -> list[int]:
    rows = store.many(
        connection,
        "SELECT m.observation_id FROM event_memberships m WHERE m.event_id=? AND m.change_type='joined' "
        "AND NOT EXISTS(SELECT 1 FROM event_memberships x WHERE x.observation_id=m.observation_id "
        "AND x.change_type='left' AND x.membership_id>m.membership_id) ORDER BY m.observation_id",
        (event_id,))
    return [int(row["observation_id"]) for row in rows]


def get_event(connection, event_id: int) -> dict[str, Any] | None:
    event = store.one(connection, "SELECT * FROM events WHERE event_id=?", (event_id,))
    if event is None:
        return None
    event["observation_ids"] = current_member_ids(connection, event_id)
    return event


def list_events(connection, *, region: str | None = None, status: str | None = None,
                limit: int = 100, after_id: int = 0) -> list[dict[str, Any]]:
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise EventError("limit", "查询上限须在 1 到 1000 之间")
    if type(after_id) is not int or after_id < 0:
        raise EventError("after_id", "查询游标无效")
    query = "SELECT * FROM events WHERE event_id>? "
    args: list[Any] = [after_id]
    if region is not None:
        query += "AND region=? "
        args.append(region)
    if status is not None:
        query += "AND status=? "
        args.append(status)
    query += "ORDER BY event_id LIMIT ?"
    args.append(limit)
    events = store.many(connection, query, tuple(args))
    for event in events:
        event["observation_ids"] = current_member_ids(connection, event["event_id"])
    return events


def _require_active_event(connection, event_id: int) -> dict[str, Any]:
    event = store.one(connection, "SELECT * FROM events WHERE event_id=?", (event_id,))
    if event is None:
        raise EventError("event_missing", f"事件 {event_id} 不存在")
    if event["status"] != "active":
        raise EventError("event_not_active", f"事件 {event_id} 已归并，不能再操作")
    return event


# --------------------------------------------------------------------------- 待办代际


def _subscription_entitled(connection, subscription: dict[str, Any], observation: dict[str, Any]) -> bool:
    if not subscription["active"]:
        return False
    if observation["region"] != subscription["region"]:
        return False
    if observation["severity"] < subscription["minimum_severity"]:
        return False
    return Decimal(observation["confidence"]) >= Decimal(subscription["minimum_confidence"])


def _next_generation(connection, event_id: int, subscription_id: str) -> int:
    row = store.one(connection,
                    "SELECT COALESCE(MAX(generation),0)+1 AS generation FROM event_duties "
                    "WHERE event_id=? AND subscription_id=?", (event_id, subscription_id))
    return int(row["generation"])


def arrange_event_duties(connection, event_id: int, when: str) -> list[int]:
    """按当前成员与当前订阅覆盖重排某事件的待办。

    对每个覆盖任一当前成员的启用订阅：旧 pending 代际标记 superseded；
    只要仍存在 pending 交接义务，就建立新一代 pending 待办并快照全部覆盖成员。
    已确认（acknowledged）代际原样保留，责任历史不丢失。
    """
    created: list[int] = []
    member_ids = current_member_ids(connection, event_id)
    if not member_ids:
        return created
    members = {row["observation_id"]: row for row in store.many(
        connection, "SELECT * FROM observations WHERE observation_id IN (%s)"
        % ",".join("?" * len(member_ids)), tuple(member_ids))}
    subscriptions = {row["subscription_id"]: row for row in store.many(
        connection, "SELECT * FROM subscriptions WHERE active=1")}
    connection.execute(
        "UPDATE event_duties SET status='superseded' WHERE event_id=? AND status='pending'", (event_id,))
    for subscription_id, subscription in sorted(subscriptions.items()):
        entitled = [oid for oid in member_ids
                    if _subscription_entitled(connection, subscription, members[oid])]
        if not entitled:
            continue
        pending_handoffs = store.many(
            connection,
            "SELECT h.handoff_id,h.observation_id FROM handoffs h WHERE h.subscription_id=? "
            "AND h.status='pending' AND h.observation_id IN (%s)"
            % ",".join("?" * len(entitled)), (subscription_id, *entitled))
        if not pending_handoffs:
            # 全部义务此前已确认：不产生新待办，确认历史保留在旧代际中。
            continue
        pending_by_observation = {row["observation_id"]: row["handoff_id"] for row in pending_handoffs}
        generation = _next_generation(connection, event_id, subscription_id)
        cursor = connection.execute(
            "INSERT INTO event_duties(event_id,subscription_id,generation,status,created_at) "
            "VALUES(?, ?, ?, 'pending', ?)", (event_id, subscription_id, generation, when))
        duty_id = int(cursor.lastrowid)
        for observation_id in entitled:
            handoff_row = store.one(
                connection, "SELECT handoff_id,status FROM handoffs WHERE observation_id=? AND subscription_id=?",
                (observation_id, subscription_id))
            if handoff_row is None:
                continue
            connection.execute(
                "INSERT INTO duty_observations(duty_id,observation_id,handoff_id,was_pending) VALUES(?,?,?,?)",
                (duty_id, observation_id, handoff_row["handoff_id"],
                 int(handoff_row["status"] == "pending")))
        created.append(duty_id)
        store.record_activity(connection, "duty_generated", "duty", str(duty_id), when,
                              {"event_id": event_id, "subscription_id": subscription_id,
                               "generation": generation, "observations": entitled})
    return created


def arrange_all_duties(connection, when: str) -> list[int]:
    """订阅重新启用等全局变化时，重排全部 active 事件。"""
    created: list[int] = []
    for row in store.many(connection, "SELECT event_id FROM events WHERE status='active' ORDER BY event_id"):
        created.extend(arrange_event_duties(connection, row["event_id"], when))
    return created


def acknowledge_duty(connection, duty_id: int, operator: str, *, when: str | None = None) -> dict[str, Any]:
    timestamp = _now(when)
    operator = _operator(operator)
    if type(duty_id) is not int or duty_id < 1:
        raise EventError("duty_id", "待办编号无效")
    with store.transaction(connection):
        duty = store.one(connection, "SELECT * FROM event_duties WHERE duty_id=?", (duty_id,))
        if duty is None:
            raise EventError("duty_missing", "事件待办不存在")
        if duty["status"] == "acknowledged":
            if duty["operator"] != operator:
                raise EventError("already_acknowledged", "待办已由其他人员确认")
            return {**duty, "status": "duplicate"}
        if duty["status"] != "pending":
            raise EventError("duty_not_pending", "待办已被重排，不能确认")
        snapshot = store.many(
            connection,
            "SELECT do2.*,h.status AS handoff_status FROM duty_observations do2 "
            "JOIN handoffs h USING(handoff_id) WHERE do2.duty_id=?", (duty_id,))
        still_pending = [row for row in snapshot if row["handoff_status"] == "pending"]
        if not still_pending:
            raise EventError("duty_empty", "待办已无未履行义务")
        for row in still_pending:
            connection.execute(
                "UPDATE handoffs SET status='acknowledged',acknowledged_at=?,operator=? "
                "WHERE handoff_id=? AND status='pending'", (timestamp, operator, row["handoff_id"]))
            store.record_activity(connection, "handoff_acknowledged", "handoff", str(row["handoff_id"]),
                                  timestamp, {"operator": operator, "duty_id": duty_id})
        connection.execute(
            "UPDATE event_duties SET status='acknowledged',acknowledged_at=?,operator=? WHERE duty_id=?",
            (timestamp, operator, duty_id))
        covered = [row["observation_id"] for row in snapshot]
        store.record_activity(connection, "duty_acknowledged", "duty", str(duty_id), timestamp,
                              {"operator": operator, "event_id": duty["event_id"],
                               "subscription_id": duty["subscription_id"],
                               "generation": duty["generation"], "covered_observations": covered})
    return duty_detail(connection, duty_id)


# --------------------------------------------------------------------------- 聚合与拆分


def merge_events(connection, event_ids: list[int], operator: str, reason: str,
                 *, when: str | None = None, proposal_id: int | None = None) -> dict[str, Any]:
    """把多个 active 事件聚合为一个保留事件（取最小编号），其余事件标记 absorbed。"""
    timestamp = _now(when)
    operator = _operator(operator)
    if not isinstance(reason, str) or not 1 <= len(reason.strip()) <= 500:
        raise EventError("reason", "聚合理由无效")
    ids = sorted({int(value) for value in event_ids})
    if len(ids) < 2:
        raise EventError("merge_targets", "聚合至少需要两个不同事件")
    with store.transaction(connection):
        return _merge_events_locked(connection, ids, operator, reason.strip(), timestamp, proposal_id)


def _merge_events_locked(connection, ids: list[int], operator: str, reason: str,
                         timestamp: str, proposal_id: int | None) -> dict[str, Any]:
    for event_id in ids:
        _require_active_event(connection, event_id)
    target_id = ids[0]
    absorbed = ids[1:]
    # 被归并事件的未决待办先作废，责任历史（acknowledged 代际）原样保留
    connection.execute(
        "UPDATE event_duties SET status='superseded' WHERE event_id IN (%s) AND status='pending'"
        % ",".join("?" * len(ids)), tuple(ids))
    for source_id in absorbed:
        for observation_id in current_member_ids(connection, source_id):
            connection.execute(
                "INSERT INTO event_memberships(event_id,observation_id,change_type,reason_code,"
                "changed_at,operator,left_event_id) VALUES(?,?,'left','merged',?,?,?)",
                (source_id, observation_id, timestamp, operator, source_id))
            connection.execute(
                "INSERT INTO event_memberships(event_id,observation_id,change_type,reason_code,"
                "changed_at,operator,left_event_id) VALUES(?,?,'joined','merged',?,?,?)",
                (target_id, observation_id, timestamp, operator, source_id))
        connection.execute(
            "UPDATE events SET status='absorbed',absorbed_at=?,absorbed_into=? WHERE event_id=?",
            (timestamp, target_id, source_id))
        connection.execute(
            "INSERT INTO event_lineage(operation,from_event_id,to_event_id,occurred_at,operator,reason,details_json) "
            "VALUES('merged',?,?,?,?,?,?)",
            (source_id, target_id, timestamp, operator, reason,
             json.dumps({"proposal_id": proposal_id, "merged_events": ids})))
    new_duties = arrange_event_duties(connection, target_id, timestamp)
    store.record_activity(connection, "events_merged", "event", str(target_id), timestamp,
                          {"operator": operator, "absorbed": absorbed, "reason": reason,
                           "proposal_id": proposal_id, "new_duty_ids": new_duties})
    return get_event(connection, target_id)


def split_event(connection, source_event_id: int, groups: list[list[int]], operator: str, reason: str,
                *, when: str | None = None, proposal_id: int | None = None) -> dict[str, Any]:
    """把一个 active 事件按给定观测分组拆成多个新事件；分组须恰好覆盖当前成员。"""
    timestamp = _now(when)
    operator = _operator(operator)
    if not isinstance(reason, str) or not 1 <= len(reason.strip()) <= 500:
        raise EventError("reason", "拆分理由无效")
    parsed_groups = [[int(oid) for oid in group] for group in groups]
    if len(parsed_groups) < 2 or any(not group for group in parsed_groups):
        raise EventError("split_groups", "拆分至少需要两个非空分组")
    flat = [oid for group in parsed_groups for oid in group]
    if len(set(flat)) != len(flat):
        raise EventError("split_groups", "观测在分组中重复")
    with store.transaction(connection):
        return _split_event_locked(connection, source_event_id, parsed_groups, flat,
                                   operator, reason.strip(), timestamp, proposal_id)


def _split_event_locked(connection, source_event_id: int, parsed_groups: list[list[int]],
                        flat: list[int], operator: str, reason: str,
                        timestamp: str, proposal_id: int | None) -> dict[str, Any]:
    _require_active_event(connection, source_event_id)
    members = current_member_ids(connection, source_event_id)
    if set(flat) != set(members):
        raise EventError("split_groups", "分组必须恰好覆盖事件当前全部观测")
    new_event_ids: list[int] = []
    for group in parsed_groups:
        region = store.one(connection, "SELECT region FROM observations WHERE observation_id=?",
                           (group[0],))["region"]
        cursor = connection.execute(
            "INSERT INTO events(region,status,created_at) VALUES(?,'active',?)", (region, timestamp))
        new_event_id = int(cursor.lastrowid)
        new_event_ids.append(new_event_id)
        for observation_id in group:
            connection.execute(
                "INSERT INTO event_memberships(event_id,observation_id,change_type,reason_code,"
                "changed_at,operator,left_event_id) VALUES(?,?,'left','split',?,?,?)",
                (source_event_id, observation_id, timestamp, operator, source_event_id))
            connection.execute(
                "INSERT INTO event_memberships(event_id,observation_id,change_type,reason_code,"
                "changed_at,operator,left_event_id) VALUES(?,?,'joined','split',?,?,?)",
                (new_event_id, observation_id, timestamp, operator, source_event_id))
        connection.execute(
            "INSERT INTO event_lineage(operation,from_event_id,to_event_id,occurred_at,operator,reason,details_json) "
            "VALUES('split',?,?,?,?,?,?)",
            (source_event_id, new_event_id, timestamp, operator, reason,
             json.dumps({"proposal_id": proposal_id, "observation_ids": group})))
    connection.execute(
        "UPDATE events SET status='absorbed',absorbed_at=?,absorbed_into=? WHERE event_id=?",
        (timestamp, new_event_ids[0], source_event_id))
    connection.execute(
        "UPDATE event_duties SET status='superseded' WHERE event_id=? AND status='pending'",
        (source_event_id,))
    new_duties: list[int] = []
    for new_event_id in new_event_ids:
        new_duties.extend(arrange_event_duties(connection, new_event_id, timestamp))
    store.record_activity(connection, "event_split", "event", str(source_event_id), timestamp,
                          {"operator": operator, "new_events": new_event_ids, "reason": reason,
                           "proposal_id": proposal_id, "new_duty_ids": new_duties})
    return {"source_event_id": source_event_id, "events": [get_event(connection, eid) for eid in new_event_ids]}


# --------------------------------------------------------------------------- 提案（确认式聚合/拆分）


def propose_merge(connection, event_ids: list[int], operator: str, reason: str,
                  *, when: str | None = None) -> dict[str, Any]:
    timestamp = _now(when)
    operator = _operator(operator)
    ids = sorted({int(value) for value in event_ids})
    if len(ids) < 2:
        raise EventError("merge_targets", "聚合至少需要两个不同事件")
    with store.transaction(connection):
        for event_id in ids:
            _require_active_event(connection, event_id)
        return _insert_proposal(connection, "merge", {"event_ids": ids}, reason, operator, timestamp)


def propose_split(connection, event_id: int, groups: list[list[int]], operator: str, reason: str,
                  *, when: str | None = None) -> dict[str, Any]:
    timestamp = _now(when)
    operator = _operator(operator)
    parsed_groups = [[int(oid) for oid in group] for group in groups]
    if len(parsed_groups) < 2 or any(not group for group in parsed_groups):
        raise EventError("split_groups", "拆分至少需要两个非空分组")
    with store.transaction(connection):
        _require_active_event(connection, event_id)
        members = current_member_ids(connection, event_id)
        flat = [oid for group in parsed_groups for oid in group]
        if len(set(flat)) != len(flat) or set(flat) != set(members):
            raise EventError("split_groups", "分组必须恰好覆盖事件当前全部观测且不重复")
        return _insert_proposal(connection, "split", {"event_id": event_id, "groups": parsed_groups},
                                reason, operator, timestamp)


def _insert_proposal(connection, kind: str, payload: dict[str, Any], reason: str,
                     operator: str, timestamp: str) -> dict[str, Any]:
    if not isinstance(reason, str) or not 1 <= len(reason.strip()) <= 500:
        raise EventError("reason", "提案理由无效")
    cursor = connection.execute(
        "INSERT INTO event_proposals(kind,status,payload_json,reason,operator,created_at) "
        "VALUES(?, 'proposed', ?, ?, ?, ?)",
        (kind, json.dumps(payload, ensure_ascii=False, sort_keys=True), reason.strip(), operator, timestamp))
    proposal_id = int(cursor.lastrowid)
    store.record_activity(connection, "proposal_created", "proposal", str(proposal_id), timestamp,
                          {"kind": kind, "operator": operator})
    return get_proposal(connection, proposal_id)


def get_proposal(connection, proposal_id: int) -> dict[str, Any]:
    row = store.one(connection, "SELECT * FROM event_proposals WHERE proposal_id=?", (proposal_id,))
    if row is None:
        raise EventError("proposal_missing", "提案不存在")
    row["payload"] = json.loads(row.pop("payload_json"))
    if row["resulting_event_ids_json"]:
        row["resulting_event_ids"] = json.loads(row["resulting_event_ids_json"])
    else:
        row["resulting_event_ids"] = []
    return row


def list_proposals(connection, *, status: str | None = None, limit: int = 100,
                   after_id: int = 0) -> list[dict[str, Any]]:
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise EventError("limit", "查询上限须在 1 到 1000 之间")
    query = "SELECT * FROM event_proposals WHERE proposal_id>? "
    args: list[Any] = [after_id]
    if status is not None:
        if status not in {"proposed", "confirmed", "cancelled"}:
            raise EventError("status", "提案状态无效")
        query += "AND status=? "
        args.append(status)
    query += "ORDER BY proposal_id LIMIT ?"
    args.append(limit)
    rows = store.many(connection, query, tuple(args))
    for row in rows:
        row["payload"] = json.loads(row.pop("payload_json"))
        row["resulting_event_ids"] = (json.loads(row.pop("resulting_event_ids_json"))
                                      if row["resulting_event_ids_json"] else [])
    return rows


def confirm_proposal(connection, proposal_id: int, operator: str, *, when: str | None = None) -> dict[str, Any]:
    timestamp = _now(when)
    operator = _operator(operator)
    with store.transaction(connection):
        proposal = store.one(connection, "SELECT * FROM event_proposals WHERE proposal_id=?", (proposal_id,))
        if proposal is None:
            raise EventError("proposal_missing", "提案不存在")
        if proposal["status"] != "proposed":
            raise EventError("proposal_decided", "提案已处理，不能重复确认")
        payload = json.loads(proposal["payload_json"])
        if proposal["kind"] == "merge":
            ids = sorted({int(value) for value in payload["event_ids"]})
            result = _merge_events_locked(connection, ids, operator, proposal["reason"],
                                          timestamp, proposal_id)
            resulting = [result["event_id"]]
        else:
            parsed_groups = [[int(oid) for oid in group] for group in payload["groups"]]
            flat = [oid for group in parsed_groups for oid in group]
            result = _split_event_locked(connection, int(payload["event_id"]), parsed_groups, flat,
                                         operator, proposal["reason"], timestamp, proposal_id)
            resulting = [event["event_id"] for event in result["events"]]
        connection.execute(
            "UPDATE event_proposals SET status='confirmed',decided_at=?,resulting_event_ids_json=? "
            "WHERE proposal_id=?", (timestamp, json.dumps(resulting), proposal_id))
        store.record_activity(connection, "proposal_confirmed", "proposal", str(proposal_id), timestamp,
                              {"kind": proposal["kind"], "operator": operator, "result": resulting})
    return get_proposal(connection, proposal_id)


def cancel_proposal(connection, proposal_id: int, operator: str, *, when: str | None = None) -> dict[str, Any]:
    timestamp = _now(when)
    operator = _operator(operator)
    with store.transaction(connection):
        proposal = store.one(connection, "SELECT * FROM event_proposals WHERE proposal_id=?", (proposal_id,))
        if proposal is None:
            raise EventError("proposal_missing", "提案不存在")
        if proposal["status"] != "proposed":
            raise EventError("proposal_decided", "提案已处理，不能撤销")
        connection.execute(
            "UPDATE event_proposals SET status='cancelled',decided_at=? WHERE proposal_id=?",
            (timestamp, proposal_id))
        store.record_activity(connection, "proposal_cancelled", "proposal", str(proposal_id), timestamp,
                              {"operator": operator})
    return get_proposal(connection, proposal_id)


# --------------------------------------------------------------------------- 定位修正


def correct_observation(connection, observation_id: int, changes: dict[str, Any], operator: str, reason: str,
                        *, when: str | None = None) -> dict[str, Any]:
    """修正观测的区域或坐标；原始值在 observation_corrections 留痕，覆盖变化时重排义务与待办。"""
    timestamp = _now(when)
    operator = _operator(operator)
    if not isinstance(reason, str) or not 1 <= len(reason.strip()) <= 500:
        raise EventError("reason", "修正理由无效")
    allowed = {"region", "latitude", "longitude"}
    if not isinstance(changes, dict) or not changes or set(changes) - allowed:
        raise EventError("changes", "只允许修正 region、latitude、longitude")
    with store.transaction(connection):
        observation = store.get_observation(connection, observation_id)
        if observation is None:
            raise EventError("observation_missing", "观测不存在")
        new_region = observation["region"]
        if "region" in changes:
            region = changes["region"]
            if not isinstance(region, str) or not REGIONS.fullmatch(region):
                raise EventError("region", "区域标识无效")
            new_region = region
        new_latitude = observation["latitude"]
        new_longitude = observation["longitude"]
        if "latitude" in changes or "longitude" in changes:
            from .normalize import _decimal
            try:
                if "latitude" in changes:
                    new_latitude = _decimal(changes["latitude"], "-90", "90", 5, 0, "latitude")
                if "longitude" in changes:
                    new_longitude = _decimal(changes["longitude"], "-180", "180", 5, 0, "longitude")
            except Exception as exc:
                raise EventError("coordinate", f"坐标无效：{exc}") from exc
        for field, new_value in (("region", new_region), ("latitude", new_latitude),
                                 ("longitude", new_longitude)):
            old_value = observation[field]
            if str(new_value) != str(old_value):
                connection.execute(
                    "INSERT INTO observation_corrections(observation_id,field_name,old_value,new_value,"
                    "reason,operator,corrected_at) VALUES(?,?,?,?,?,?,?)",
                    (observation_id, field, str(old_value), str(new_value), reason.strip(), operator, timestamp))
        connection.execute(
            "UPDATE observations SET region=?,latitude=?,longitude=? WHERE observation_id=?",
            (new_region, new_latitude, new_longitude, observation_id))
        affected_events: list[int] = []
        if new_region != observation["region"]:
            affected_event = current_event_id(connection, observation_id)
            # 覆盖即将变化：先作废引用旧义务的未决待办，再重算义务与待办
            if affected_event is not None:
                connection.execute(
                    "UPDATE event_duties SET status='superseded' WHERE event_id=? AND status='pending'",
                    (affected_event,))
            affected_events = _recover_coverage(connection, observation_id, new_region, timestamp, operator)
            connection.execute(
                "INSERT INTO event_lineage(operation,from_event_id,to_event_id,occurred_at,operator,reason,details_json) "
                "VALUES('corrected',?,?,?,?,?,?)",
                (affected_event, affected_event, timestamp, operator, reason.strip(),
                 json.dumps({"observation_id": observation_id, "old_region": observation["region"],
                             "new_region": new_region})))
        event_id = current_event_id(connection, observation_id)
        if event_id is not None and new_region != observation["region"]:
            arrange_event_duties(connection, event_id, timestamp)
        store.record_activity(connection, "observation_corrected", "observation", str(observation_id), timestamp,
                              {"operator": operator, "changes": changes, "affected_events": affected_events})
    return audit_observation(connection, observation_id)


def _recover_coverage(connection, observation_id: int, new_region: str,
                      timestamp: str, operator: str) -> list[int]:
    """区域修正后按当前订阅重新判定每条接收义务：新增、撤销、恢复，绝不删除行。

    handoffs 对（观测,订阅）有唯一约束，因此义务是同一行上的状态机
    pending -> acknowledged/withdrawn -> pending；撤销与恢复的完整轨迹由
    activity 与 observation_corrections 留痕，行本身永不删除。
    """
    observation = store.get_observation(connection, observation_id)
    affected: list[int] = []
    for subscription in store.many(connection, "SELECT * FROM subscriptions ORDER BY subscription_id"):
        existing = store.one(connection,
                             "SELECT * FROM handoffs WHERE observation_id=? AND subscription_id=?",
                             (observation_id, subscription["subscription_id"]))
        entitled = _subscription_entitled(connection, subscription, observation)
        if entitled and subscription["active"] and existing is None:
            cursor = connection.execute(
                "INSERT INTO handoffs(observation_id,subscription_id,status,created_at) "
                "VALUES(?,?,'pending',?)", (observation_id, subscription["subscription_id"], timestamp))
            handoff_id = int(cursor.lastrowid)
            store.record_activity(connection, "handoff_created", "handoff", str(handoff_id), timestamp,
                                  {"observation_id": observation_id,
                                   "subscription_id": subscription["subscription_id"], "reason": "corrected"})
        elif entitled and existing is not None and existing["status"] == "withdrawn":
            # 覆盖恢复：复用同一行置回 pending（撤销事实已在 activity 留痕，行未删除）
            connection.execute(
                "UPDATE handoffs SET status='pending' WHERE handoff_id=? AND status='withdrawn'",
                (existing["handoff_id"],))
            store.record_activity(connection, "handoff_restored", "handoff", str(existing["handoff_id"]),
                                  timestamp, {"operator": operator, "reason": "coverage_restored"})
        elif not entitled and existing is not None and existing["status"] == "pending":
            connection.execute(
                "UPDATE handoffs SET status='withdrawn' WHERE handoff_id=? AND status='pending'",
                (existing["handoff_id"],))
            store.record_activity(connection, "handoff_withdrawn", "handoff", str(existing["handoff_id"]),
                                  timestamp, {"operator": operator, "reason": "coverage_changed"})
        event_id = current_event_id(connection, observation_id)
        if event_id is not None and event_id not in affected:
            affected.append(event_id)
    return affected


# --------------------------------------------------------------------------- 查询


def list_duties(connection, *, subscription_id: str | None = None, event_id: int | None = None,
                status: str | None = None, limit: int = 100, after_id: int = 0) -> list[dict[str, Any]]:
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise EventError("limit", "查询上限须在 1 到 1000 之间")
    if type(after_id) is not int or after_id < 0:
        raise EventError("after_id", "查询游标无效")
    if status is not None and status not in {"pending", "acknowledged", "superseded"}:
        raise EventError("status", "待办状态无效")
    query = "SELECT * FROM event_duties WHERE duty_id>? "
    args: list[Any] = [after_id]
    if subscription_id is not None:
        query += "AND subscription_id=? "
        args.append(subscription_id)
    if event_id is not None:
        query += "AND event_id=? "
        args.append(event_id)
    if status is not None:
        query += "AND status=? "
        args.append(status)
    query += "ORDER BY duty_id LIMIT ?"
    args.append(limit)
    return store.many(connection, query, tuple(args))


def duty_detail(connection, duty_id: int) -> dict[str, Any] | None:
    duty = store.one(connection, "SELECT * FROM event_duties WHERE duty_id=?", (duty_id,))
    if duty is None:
        return None
    duty["snapshot"] = store.many(
        connection,
        "SELECT do2.observation_id,do2.handoff_id,do2.was_pending,h.status AS handoff_status,h.operator "
        "AS handoff_operator,h.acknowledged_at AS handoff_acknowledged_at,o.external_id,o.region,o.source "
        "FROM duty_observations do2 JOIN handoffs h USING(handoff_id) "
        "JOIN observations o ON o.observation_id=do2.observation_id WHERE do2.duty_id=? ORDER BY do2.observation_id",
        (duty_id,))
    return duty


def event_detail(connection, event_id: int) -> dict[str, Any] | None:
    event = get_event(connection, event_id)
    if event is None:
        return None
    event["memberships"] = store.many(
        connection, "SELECT * FROM event_memberships WHERE event_id=? ORDER BY membership_id", (event_id,))
    event["lineage"] = store.many(
        connection,
        "SELECT * FROM event_lineage WHERE from_event_id=? OR to_event_id=? ORDER BY lineage_id",
        (event_id, event_id))
    for row in event["lineage"]:
        row["details"] = json.loads(row.pop("details_json"))
    event["duties"] = store.many(
        connection, "SELECT * FROM event_duties WHERE event_id=? ORDER BY duty_id", (event_id,))
    event["member_observations"] = store.many(
        connection,
        "SELECT o.observation_id,o.external_id,o.station_id,o.region,o.latitude,o.longitude,o.confidence,"
        "o.severity,o.source,o.observed_at FROM observations o WHERE o.observation_id IN (%s) "
        "ORDER BY o.observation_id" % ",".join("?" * len(event["observation_ids"])),
        tuple(event["observation_ids"])) if event["observation_ids"] else []
    return event


def audit_observation(connection, observation_id: int) -> dict[str, Any] | None:
    observation = store.get_observation(connection, observation_id)
    if observation is None:
        return None
    return {
        "observation": observation,
        "current_event_id": current_event_id(connection, observation_id),
        "memberships": store.many(
            connection, "SELECT * FROM event_memberships WHERE observation_id=? ORDER BY membership_id",
            (observation_id,)),
        "handoffs": store.many(
            connection, "SELECT * FROM handoffs WHERE observation_id=? ORDER BY handoff_id", (observation_id,)),
        "corrections": store.many(
            connection, "SELECT * FROM observation_corrections WHERE observation_id=? ORDER BY correction_id",
            (observation_id,)),
    }


def event_overview(connection) -> dict[str, Any]:
    """分别解释原始观测、当前事件与每组未履行义务；计数只增不删。"""
    totals = store.one(connection,
                       "SELECT (SELECT COUNT(*) FROM observations) AS raw_observations,"
                       "(SELECT COUNT(*) FROM events) AS total_events,"
                       "(SELECT COUNT(*) FROM events WHERE status='active') AS active_events,"
                       "(SELECT COUNT(*) FROM events WHERE status='absorbed') AS absorbed_events,"
                       "(SELECT COUNT(*) FROM handoffs) AS total_handoffs,"
                       "(SELECT COUNT(*) FROM handoffs WHERE status='pending') AS pending_handoffs,"
                       "(SELECT COUNT(*) FROM handoffs WHERE status='acknowledged') AS acknowledged_handoffs,"
                       "(SELECT COUNT(*) FROM handoffs WHERE status='withdrawn') AS withdrawn_handoffs,"
                       "(SELECT COUNT(*) FROM event_duties WHERE status='pending') AS pending_duties")
    groups = store.many(
        connection,
        "SELECT s.subscription_id,s.region,s.active,"
        "SUM(CASE WHEN h.status='pending' THEN 1 ELSE 0 END) AS pending_obligations,"
        "SUM(CASE WHEN h.status='acknowledged' THEN 1 ELSE 0 END) AS acknowledged_obligations,"
        "SUM(CASE WHEN h.status='withdrawn' THEN 1 ELSE 0 END) AS withdrawn_obligations,"
        "COUNT(h.handoff_id) AS total_obligations,"
        "(SELECT COUNT(*) FROM event_duties d WHERE d.subscription_id=s.subscription_id "
        "AND d.status='pending') AS pending_duty_count "
        "FROM subscriptions s LEFT JOIN handoffs h ON h.subscription_id=s.subscription_id "
        "GROUP BY s.subscription_id ORDER BY s.subscription_id")
    return {**totals, "subscriptions": groups}


def find_duty_gaps(connection) -> list[dict[str, Any]]:
    """核对：启用订阅下每条 pending 交接义务都必须被某个 pending 待办快照覆盖。"""
    gaps = store.many(
        connection,
        "SELECT h.handoff_id,h.observation_id,h.subscription_id FROM handoffs h "
        "JOIN observations o USING(observation_id) JOIN subscriptions s USING(subscription_id) "
        "WHERE h.status='pending' AND s.active=1 AND NOT EXISTS("
        "SELECT 1 FROM duty_observations do2 JOIN event_duties d ON d.duty_id=do2.duty_id "
        "WHERE do2.handoff_id=h.handoff_id AND d.status='pending') ORDER BY h.handoff_id")
    return gaps
