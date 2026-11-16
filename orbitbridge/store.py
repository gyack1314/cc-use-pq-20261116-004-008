"""SQLite 持久化边界与原子事务。"""
from __future__ import annotations

import contextlib
import json
import sqlite3
from pathlib import Path
from typing import Any, Iterator

SCHEMA = """
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS stations (
  station_id TEXT PRIMARY KEY, label TEXT NOT NULL, active INTEGER NOT NULL CHECK(active IN (0,1)),
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS subscriptions (
  subscription_id TEXT PRIMARY KEY, region TEXT NOT NULL, minimum_severity INTEGER NOT NULL
    CHECK(minimum_severity BETWEEN 1 AND 5), minimum_confidence TEXT NOT NULL,
  active INTEGER NOT NULL CHECK(active IN (0,1)), created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS subscriptions_region ON subscriptions(region, active);
CREATE TABLE IF NOT EXISTS batches (
  batch_id INTEGER PRIMARY KEY AUTOINCREMENT, station_id TEXT NOT NULL REFERENCES stations(station_id),
  sequence INTEGER NOT NULL CHECK(sequence > 0), packet_id TEXT NOT NULL, digest TEXT NOT NULL,
  packet_time TEXT NOT NULL, received_at TEXT NOT NULL, source_name TEXT NOT NULL,
  record_count INTEGER NOT NULL, UNIQUE(station_id, sequence), UNIQUE(packet_id)
);
CREATE TABLE IF NOT EXISTS station_cursors (
  station_id TEXT PRIMARY KEY REFERENCES stations(station_id), committed_sequence INTEGER NOT NULL DEFAULT 0,
  last_batch_id INTEGER REFERENCES batches(batch_id), updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS observations (
  observation_id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id INTEGER NOT NULL REFERENCES batches(batch_id),
  station_id TEXT NOT NULL REFERENCES stations(station_id), external_id TEXT NOT NULL, observed_at TEXT NOT NULL,
  region TEXT NOT NULL, latitude TEXT NOT NULL, longitude TEXT NOT NULL, confidence TEXT NOT NULL,
  severity INTEGER NOT NULL, source TEXT NOT NULL, fingerprint TEXT NOT NULL,
  UNIQUE(station_id, external_id), UNIQUE(batch_id, external_id)
);
CREATE INDEX IF NOT EXISTS observations_region ON observations(region, observed_at);
CREATE TABLE IF NOT EXISTS handoffs (
  handoff_id INTEGER PRIMARY KEY AUTOINCREMENT, observation_id INTEGER NOT NULL REFERENCES observations(observation_id),
  subscription_id TEXT NOT NULL REFERENCES subscriptions(subscription_id),
  status TEXT NOT NULL CHECK(status IN ('pending','acknowledged','withdrawn')),
  created_at TEXT NOT NULL, acknowledged_at TEXT, operator TEXT,
  UNIQUE(observation_id, subscription_id),
  CHECK((status='pending' AND acknowledged_at IS NULL AND operator IS NULL) OR
        (status='acknowledged' AND acknowledged_at IS NOT NULL AND operator IS NOT NULL) OR
        (status='withdrawn' AND acknowledged_at IS NULL AND operator IS NULL))
);
CREATE INDEX IF NOT EXISTS handoffs_status ON handoffs(status, subscription_id);
CREATE TABLE IF NOT EXISTS events (
  event_id INTEGER PRIMARY KEY AUTOINCREMENT, region TEXT NOT NULL, status TEXT NOT NULL
    CHECK(status IN ('active','absorbed')), created_at TEXT NOT NULL, absorbed_at TEXT,
  absorbed_into INTEGER REFERENCES events(event_id)
);
CREATE INDEX IF NOT EXISTS events_region ON events(region, status);
CREATE TABLE IF NOT EXISTS event_memberships (
  membership_id INTEGER PRIMARY KEY AUTOINCREMENT, event_id INTEGER NOT NULL REFERENCES events(event_id),
  observation_id INTEGER NOT NULL REFERENCES observations(observation_id),
  change_type TEXT NOT NULL CHECK(change_type IN ('joined','left')), reason_code TEXT NOT NULL,
  changed_at TEXT NOT NULL, operator TEXT, left_event_id INTEGER REFERENCES events(event_id),
  UNIQUE(membership_id, change_type)
);
CREATE INDEX IF NOT EXISTS memberships_observation ON event_memberships(observation_id, membership_id);
CREATE INDEX IF NOT EXISTS memberships_event ON event_memberships(event_id, membership_id);
CREATE TABLE IF NOT EXISTS event_lineage (
  lineage_id INTEGER PRIMARY KEY AUTOINCREMENT, operation TEXT NOT NULL
    CHECK(operation IN ('created','merged','split','corrected')),
  from_event_id INTEGER REFERENCES events(event_id), to_event_id INTEGER REFERENCES events(event_id),
  occurred_at TEXT NOT NULL, operator TEXT, reason TEXT, details_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS lineage_from ON event_lineage(from_event_id, lineage_id);
CREATE INDEX IF NOT EXISTS lineage_to ON event_lineage(to_event_id, lineage_id);
CREATE TABLE IF NOT EXISTS event_duties (
  duty_id INTEGER PRIMARY KEY AUTOINCREMENT, event_id INTEGER NOT NULL REFERENCES events(event_id),
  subscription_id TEXT NOT NULL REFERENCES subscriptions(subscription_id),
  generation INTEGER NOT NULL CHECK(generation >= 1),
  status TEXT NOT NULL CHECK(status IN ('pending','acknowledged','superseded')),
  created_at TEXT NOT NULL, acknowledged_at TEXT, operator TEXT,
  UNIQUE(event_id, subscription_id, generation),
  CHECK((status='pending' AND acknowledged_at IS NULL AND operator IS NULL) OR
        (status='acknowledged' AND acknowledged_at IS NOT NULL AND operator IS NOT NULL) OR
        (status='superseded' AND acknowledged_at IS NULL AND operator IS NULL))
);
CREATE INDEX IF NOT EXISTS duties_status ON event_duties(status, subscription_id);
CREATE INDEX IF NOT EXISTS duties_event ON event_duties(event_id, generation);
CREATE TABLE IF NOT EXISTS duty_observations (
  duty_id INTEGER NOT NULL REFERENCES event_duties(duty_id),
  observation_id INTEGER NOT NULL REFERENCES observations(observation_id),
  handoff_id INTEGER NOT NULL REFERENCES handoffs(handoff_id),
  was_pending INTEGER NOT NULL CHECK(was_pending IN (0,1)),
  PRIMARY KEY(duty_id, observation_id)
);
CREATE INDEX IF NOT EXISTS duty_observations_observation ON duty_observations(observation_id);
CREATE TABLE IF NOT EXISTS event_proposals (
  proposal_id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL CHECK(kind IN ('merge','split')),
  status TEXT NOT NULL CHECK(status IN ('proposed','confirmed','cancelled')),
  payload_json TEXT NOT NULL, reason TEXT NOT NULL, operator TEXT NOT NULL,
  created_at TEXT NOT NULL, decided_at TEXT, resulting_event_ids_json TEXT
);
CREATE INDEX IF NOT EXISTS proposals_status ON event_proposals(status, proposal_id);
CREATE TABLE IF NOT EXISTS observation_corrections (
  correction_id INTEGER PRIMARY KEY AUTOINCREMENT, observation_id INTEGER NOT NULL REFERENCES observations(observation_id),
  field_name TEXT NOT NULL, old_value TEXT NOT NULL, new_value TEXT NOT NULL,
  reason TEXT NOT NULL, operator TEXT NOT NULL, corrected_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS corrections_observation ON observation_corrections(observation_id, correction_id);
CREATE TABLE IF NOT EXISTS quarantine (
  quarantine_id INTEGER PRIMARY KEY AUTOINCREMENT, station_id TEXT, sequence INTEGER, source_name TEXT NOT NULL,
  content_digest TEXT NOT NULL, reason_code TEXT NOT NULL, detail TEXT NOT NULL,
  first_seen TEXT NOT NULL, last_seen TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 1,
  UNIQUE(station_id, sequence, content_digest, reason_code)
);
CREATE TABLE IF NOT EXISTS activity (
  activity_id INTEGER PRIMARY KEY AUTOINCREMENT, event_type TEXT NOT NULL, entity_type TEXT NOT NULL,
  entity_id TEXT NOT NULL, occurred_at TEXT NOT NULL, details_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS activity_entity ON activity(entity_type, entity_id, activity_id);
"""


def _migrate(connection: sqlite3.Connection) -> None:
    """升级既有数据库：handoffs 增加 withdrawn 状态，事件层表由 SCHEMA 幂等创建。"""
    constraint = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='handoffs'").fetchone()
    if constraint is None or "withdrawn" in constraint[0]:
        return
    # 重命名基表期间临时关闭外键，避免 schema 缓存指向旧表名
    connection.execute("PRAGMA foreign_keys=OFF")
    try:
        connection.executescript(
            """
            ALTER TABLE handoffs RENAME TO handoffs_legacy;
            CREATE TABLE handoffs (
              handoff_id INTEGER PRIMARY KEY AUTOINCREMENT, observation_id INTEGER NOT NULL
                REFERENCES observations(observation_id),
              subscription_id TEXT NOT NULL REFERENCES subscriptions(subscription_id),
              status TEXT NOT NULL CHECK(status IN ('pending','acknowledged','withdrawn')),
              created_at TEXT NOT NULL, acknowledged_at TEXT, operator TEXT,
              UNIQUE(observation_id, subscription_id),
              CHECK((status='pending' AND acknowledged_at IS NULL AND operator IS NULL) OR
                    (status='acknowledged' AND acknowledged_at IS NOT NULL AND operator IS NOT NULL) OR
                    (status='withdrawn' AND acknowledged_at IS NULL AND operator IS NULL))
            );
            INSERT INTO handoffs(handoff_id,observation_id,subscription_id,status,created_at,acknowledged_at,operator)
              SELECT handoff_id,observation_id,subscription_id,status,created_at,acknowledged_at,operator FROM handoffs_legacy;
            DROP TABLE handoffs_legacy;
            CREATE INDEX IF NOT EXISTS handoffs_status ON handoffs(status, subscription_id);
            """
        )
    finally:
        connection.execute("PRAGMA foreign_keys=ON")


def _backfill_event_layer(connection: sqlite3.Connection) -> None:
    """为事件层出现之前的历史观测回填单例事件、成员、血缘与待办代际。"""
    legacy = connection.execute(
        "SELECT o.observation_id,o.region,b.received_at FROM observations o JOIN batches b ON b.batch_id=o.batch_id "
        "WHERE NOT EXISTS(SELECT 1 FROM event_memberships m WHERE m.observation_id=o.observation_id) "
        "ORDER BY o.observation_id").fetchall()
    for row in legacy:
        when = row["received_at"]
        cursor = connection.execute(
            "INSERT INTO events(region,status,created_at) VALUES(?,'active',?)", (row["region"], when))
        event_id = int(cursor.lastrowid)
        member_cursor = connection.execute(
            "INSERT INTO event_memberships(event_id,observation_id,change_type,reason_code,changed_at,operator) "
            "VALUES(?,?,'joined','backfilled',?,?)", (event_id, row["observation_id"], when, "migration"))
        membership_id = int(member_cursor.lastrowid)
        connection.execute(
            "INSERT INTO event_lineage(operation,from_event_id,to_event_id,occurred_at,operator,reason,details_json) "
            "VALUES('created',NULL,?,?,?,?,?)",
            (event_id, when, "migration", "backfilled",
             json.dumps({"observation_id": row["observation_id"], "membership_id": membership_id})))
        # 为每条历史 handoff 建立对应代际：pending 的留待值守，acknowledged 的保留责任快照
        for handoff_row in connection.execute(
                "SELECT * FROM handoffs WHERE observation_id=? ORDER BY handoff_id", (row["observation_id"],)):
            existing = connection.execute(
                "SELECT 1 FROM event_duties WHERE event_id=? AND subscription_id=?",
                (event_id, handoff_row["subscription_id"])).fetchone()
            if existing:
                continue
            duty_cursor = connection.execute(
                "INSERT INTO event_duties(event_id,subscription_id,generation,status,created_at,"
                "acknowledged_at,operator) VALUES(?,?,1,?,?,?,?)",
                (event_id, handoff_row["subscription_id"], handoff_row["status"], when,
                 handoff_row["acknowledged_at"], handoff_row["operator"]))
            duty_id = int(duty_cursor.lastrowid)
            connection.execute(
                "INSERT INTO duty_observations(duty_id,observation_id,handoff_id,was_pending) VALUES(?,?,?,?)",
                (duty_id, row["observation_id"], handoff_row["handoff_id"],
                 int(handoff_row["status"] == "pending")))


def open_database(path: str | Path) -> sqlite3.Connection:
    database = Path(path)
    database.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database, timeout=10, isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA busy_timeout=10000")
    connection.execute("PRAGMA journal_mode=WAL")
    # 先迁移旧 handoffs（此时尚无事件层外键引用），再幂等创建含事件层的完整 schema
    _migrate(connection)
    connection.executescript(SCHEMA)
    _backfill_event_layer(connection)
    return connection


@contextlib.contextmanager
def transaction(connection: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    connection.execute("BEGIN IMMEDIATE")
    try:
        yield connection
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


def one(connection: sqlite3.Connection, query: str, args: tuple[Any, ...] = ()) -> dict[str, Any] | None:
    row = connection.execute(query, args).fetchone()
    return dict(row) if row is not None else None


def many(connection: sqlite3.Connection, query: str, args: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    return [dict(row) for row in connection.execute(query, args)]


def record_activity(connection: sqlite3.Connection, event_type: str, entity_type: str, entity_id: str,
                    occurred_at: str, details: dict[str, Any]) -> int:
    cursor = connection.execute(
        "INSERT INTO activity(event_type,entity_type,entity_id,occurred_at,details_json) VALUES(?,?,?,?,?)",
        (event_type, entity_type, entity_id, occurred_at, json.dumps(details, ensure_ascii=False, sort_keys=True)),
    )
    return int(cursor.lastrowid)


def insert_batch(connection: sqlite3.Connection, station_id: str, sequence: int, packet_id: str, digest: str,
                 packet_time: str, received_at: str, source_name: str, record_count: int) -> int:
    cursor = connection.execute(
        "INSERT INTO batches(station_id,sequence,packet_id,digest,packet_time,received_at,source_name,record_count) "
        "VALUES(?,?,?,?,?,?,?,?)",
        (station_id, sequence, packet_id, digest, packet_time, received_at, source_name, record_count),
    )
    return int(cursor.lastrowid)


def insert_observation(connection: sqlite3.Connection, batch_id: int, station_id: str, observation: dict[str, Any]) -> int:
    cursor = connection.execute(
        "INSERT INTO observations(batch_id,station_id,external_id,observed_at,region,latitude,longitude,"
        "confidence,severity,source,fingerprint) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        (batch_id, station_id, observation["external_id"], observation["observed_at"], observation["region"],
         observation["latitude"], observation["longitude"], observation["confidence"],
         observation["severity"], observation["source"], observation["fingerprint"]),
    )
    return int(cursor.lastrowid)


def update_cursor(connection: sqlite3.Connection, station_id: str, sequence: int, batch_id: int, when: str) -> None:
    changed = connection.execute(
        "UPDATE station_cursors SET committed_sequence=?,last_batch_id=?,updated_at=? "
        "WHERE station_id=? AND committed_sequence=?",
        (sequence, batch_id, when, station_id, sequence - 1),
    ).rowcount
    if changed != 1:
        raise RuntimeError("站点游标未能原子更新")


def quarantine(connection: sqlite3.Connection, station_id: str | None, sequence: int | None,
               source_name: str, content_digest: str, reason_code: str, detail: str, when: str) -> int:
    existing = one(connection, "SELECT quarantine_id FROM quarantine WHERE station_id IS ? AND sequence IS ? "
                   "AND content_digest=? AND reason_code=?", (station_id, sequence, content_digest, reason_code))
    if existing:
        connection.execute("UPDATE quarantine SET attempts=attempts+1,last_seen=? WHERE quarantine_id=?",
                           (when, existing["quarantine_id"]))
        return int(existing["quarantine_id"])
    cursor = connection.execute(
        "INSERT INTO quarantine(station_id,sequence,source_name,content_digest,reason_code,detail,first_seen,last_seen) "
        "VALUES(?,?,?,?,?,?,?,?)",
        (station_id, sequence, source_name, content_digest, reason_code, detail[:1000], when, when),
    )
    return int(cursor.lastrowid)


def station_cursor(connection: sqlite3.Connection, station_id: str) -> dict[str, Any] | None:
    return one(connection, "SELECT * FROM station_cursors WHERE station_id=?", (station_id,))


def batch_for_sequence(connection: sqlite3.Connection, station_id: str, sequence: int) -> dict[str, Any] | None:
    return one(connection, "SELECT * FROM batches WHERE station_id=? AND sequence=?", (station_id, sequence))


def get_batch(connection: sqlite3.Connection, batch_id: int) -> dict[str, Any] | None:
    return one(connection, "SELECT * FROM batches WHERE batch_id=?", (batch_id,))


def get_observation(connection: sqlite3.Connection, observation_id: int) -> dict[str, Any] | None:
    return one(connection, "SELECT * FROM observations WHERE observation_id=?", (observation_id,))


def get_handoff(connection: sqlite3.Connection, handoff_id: int) -> dict[str, Any] | None:
    return one(connection, "SELECT * FROM handoffs WHERE handoff_id=?", (handoff_id,))


def counts(connection: sqlite3.Connection) -> dict[str, int]:
    names = ("stations", "subscriptions", "batches", "observations", "handoffs", "events",
             "event_memberships", "event_lineage", "event_duties", "duty_observations",
             "event_proposals", "observation_corrections", "quarantine", "activity")
    return {name: int(connection.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]) for name in names}


def integrity_check(connection: sqlite3.Connection) -> list[str]:
    issues = []
    if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
        issues.append("sqlite_integrity")
    for row in connection.execute("PRAGMA foreign_key_check"):
        issues.append(f"foreign_key:{row[0]}:{row[1]}")
    for row in connection.execute(
        "SELECT c.station_id,c.committed_sequence,MAX(b.sequence) AS max_seq FROM station_cursors c "
        "LEFT JOIN batches b ON b.station_id=c.station_id GROUP BY c.station_id"
    ):
        if row["committed_sequence"] != (row["max_seq"] or 0):
            issues.append(f"cursor:{row['station_id']}")
    for row in connection.execute(
        "SELECT b.batch_id,b.record_count,COUNT(o.observation_id) AS actual FROM batches b "
        "LEFT JOIN observations o ON o.batch_id=b.batch_id GROUP BY b.batch_id"
    ):
        if row["record_count"] != row["actual"]:
            issues.append(f"batch_count:{row['batch_id']}")
    # 每条观测在任一时刻最多属于一个事件：joined 与 left 必须配对且数量一致
    for row in connection.execute(
        "SELECT observation_id FROM event_memberships GROUP BY observation_id "
        "HAVING SUM(change_type='joined') <> SUM(change_type='left') + 1"
    ):
        issues.append(f"membership_unbalanced:{row['observation_id']}")
    # 当前（最后一次为 joined）成员只能挂在 active 事件上
    for row in connection.execute(
        "SELECT m.observation_id,m.event_id FROM event_memberships m JOIN events e USING(event_id) "
        "WHERE m.membership_id=(SELECT MAX(membership_id) FROM event_memberships WHERE observation_id=m.observation_id) "
        "AND m.change_type='joined' AND e.status<>'active'"
    ):
        issues.append(f"member_on_inactive_event:{row['observation_id']}")
    # active 事件至少有一名当前成员
    for row in connection.execute(
        "SELECT e.event_id FROM events e WHERE e.status='active' AND NOT EXISTS("
        "SELECT 1 FROM event_memberships m WHERE m.event_id=e.event_id AND m.change_type='joined' "
        "AND NOT EXISTS(SELECT 1 FROM event_memberships x WHERE x.observation_id=m.observation_id "
        "AND x.change_type='left' AND x.membership_id>m.membership_id))"
    ):
        issues.append(f"empty_active_event:{row['event_id']}")
    # pending 事件待办必须仍有未履行义务（pending 交接）
    for row in connection.execute(
        "SELECT d.duty_id FROM event_duties d WHERE d.status='pending' AND NOT EXISTS("
        "SELECT 1 FROM duty_observations do2 JOIN handoffs h USING(handoff_id) "
        "WHERE do2.duty_id=d.duty_id AND h.status='pending')"
    ):
        issues.append(f"duty_without_pending:{row['duty_id']}")
    return issues
