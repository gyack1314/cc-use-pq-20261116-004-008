from __future__ import annotations

import sqlite3

from orbitbridge import quality, store
from helpers import DatabaseCase

LEGACY_SCHEMA = """
CREATE TABLE stations(station_id TEXT PRIMARY KEY,label TEXT NOT NULL,active INTEGER NOT NULL,created_at TEXT NOT NULL);
CREATE TABLE subscriptions(subscription_id TEXT PRIMARY KEY,region TEXT NOT NULL,minimum_severity INTEGER NOT NULL,
  minimum_confidence TEXT NOT NULL,active INTEGER NOT NULL,created_at TEXT NOT NULL);
CREATE TABLE batches(batch_id INTEGER PRIMARY KEY AUTOINCREMENT,station_id TEXT NOT NULL,sequence INTEGER NOT NULL,
  packet_id TEXT NOT NULL,digest TEXT NOT NULL,packet_time TEXT NOT NULL,received_at TEXT NOT NULL,
  source_name TEXT NOT NULL,record_count INTEGER NOT NULL,UNIQUE(station_id,sequence),UNIQUE(packet_id));
CREATE TABLE station_cursors(station_id TEXT PRIMARY KEY,committed_sequence INTEGER NOT NULL DEFAULT 0,
  last_batch_id INTEGER,updated_at TEXT NOT NULL);
CREATE TABLE observations(observation_id INTEGER PRIMARY KEY AUTOINCREMENT,batch_id INTEGER NOT NULL,
  station_id TEXT NOT NULL,external_id TEXT NOT NULL,observed_at TEXT NOT NULL,region TEXT NOT NULL,
  latitude TEXT NOT NULL,longitude TEXT NOT NULL,confidence TEXT NOT NULL,severity INTEGER NOT NULL,
  source TEXT NOT NULL,fingerprint TEXT NOT NULL,UNIQUE(station_id,external_id),UNIQUE(batch_id,external_id));
CREATE TABLE handoffs(handoff_id INTEGER PRIMARY KEY AUTOINCREMENT,observation_id INTEGER NOT NULL,
  subscription_id TEXT NOT NULL,status TEXT NOT NULL CHECK(status IN ('pending','acknowledged')),
  created_at TEXT NOT NULL,acknowledged_at TEXT,operator TEXT,UNIQUE(observation_id,subscription_id),
  CHECK((status='pending' AND acknowledged_at IS NULL AND operator IS NULL) OR
        (status='acknowledged' AND acknowledged_at IS NOT NULL AND operator IS NOT NULL)));
CREATE TABLE quarantine(quarantine_id INTEGER PRIMARY KEY AUTOINCREMENT,station_id TEXT,sequence INTEGER,
  source_name TEXT NOT NULL,content_digest TEXT NOT NULL,reason_code TEXT NOT NULL,detail TEXT NOT NULL,
  first_seen TEXT NOT NULL,last_seen TEXT NOT NULL,attempts INTEGER NOT NULL DEFAULT 1);
CREATE TABLE activity(activity_id INTEGER PRIMARY KEY AUTOINCREMENT,event_type TEXT NOT NULL,entity_type TEXT NOT NULL,
  entity_id TEXT NOT NULL,occurred_at TEXT NOT NULL,details_json TEXT NOT NULL);
"""


class MigrationTests(DatabaseCase):
    def setUp(self) -> None:
        # 不调用父类建库：本测试需要先写入基线 008 的旧表结构
        import tempfile
        from pathlib import Path
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.db_path = self.root / "store.sqlite3"

    def _build_legacy_database(self):
        raw = sqlite3.connect(self.db_path)
        raw.executescript(LEGACY_SCHEMA)
        raw.execute("INSERT INTO stations VALUES('sat-a','旧站',1,'2026-10-28T12:00:00Z')")
        raw.execute("INSERT INTO subscriptions VALUES('north-watch','north',3,'0.7',1,'2026-10-28T12:00:00Z')")
        raw.execute("INSERT INTO station_cursors VALUES('sat-a',0,NULL,'2026-10-28T12:00:00Z')")
        raw.commit()
        raw.close()

    def test_legacy_database_is_upgraded_and_idempotent(self):
        self._build_legacy_database()
        reopened = store.open_database(self.db_path)
        self.db = reopened
        definition = reopened.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='handoffs'").fetchone()[0]
        self.assertIn("withdrawn", definition)
        for table in ("events", "event_memberships", "event_lineage", "event_duties",
                      "duty_observations", "event_proposals", "observation_corrections"):
            self.assertIsNotNone(
                reopened.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone())
        self.assertEqual(store.integrity_check(reopened), [])
        self.assertTrue(quality.check_all(reopened)["ok"])
        # 再次打开不重复迁移、不报错
        reopened.close()
        second = store.open_database(self.db_path)
        self.db = second
        self.assertTrue(quality.check_all(second)["ok"])

    def test_legacy_acknowledged_handoff_survives_migration(self):
        self._build_legacy_database()
        raw = sqlite3.connect(self.db_path)
        raw.execute("INSERT INTO batches(station_id,sequence,packet_id,digest,packet_time,received_at,"
                    "source_name,record_count) VALUES('sat-a',1,'pkt','d','2026-10-28T12:02:00Z',"
                    "'2026-10-28T12:03:00Z','f.jsonl',1)")
        raw.execute("UPDATE station_cursors SET committed_sequence=1,last_batch_id=1 WHERE station_id='sat-a'")
        raw.execute("INSERT INTO observations(batch_id,station_id,external_id,observed_at,region,latitude,"
                    "longitude,confidence,severity,source,fingerprint) VALUES(1,'sat-a','legacy-1',"
                    "'2026-10-28T12:01:00Z','north','31.2','121.4','0.9',4,'infrared','fp')")
        raw.execute("INSERT INTO handoffs(observation_id,subscription_id,status,created_at,acknowledged_at,operator) "
                    "VALUES(1,'north-watch','acknowledged','2026-10-28T12:03:00Z','2026-10-28T12:04:00Z','旧值守员')")
        raw.commit()
        raw.close()
        reopened = store.open_database(self.db_path)
        self.db = reopened
        row = reopened.execute("SELECT status,operator FROM handoffs WHERE handoff_id=1").fetchone()
        self.assertEqual(tuple(row), ("acknowledged", "旧值守员"))
        self.assertTrue(quality.check_all(reopened)["ok"])
