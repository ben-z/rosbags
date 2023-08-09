# Copyright 2020-2023  Ternaris.
# SPDX-License-Identifier: Apache-2.0
"""Sqlite3 storage tests."""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

import pytest

from rosbags.rosbag2.errors import ReaderError
from rosbags.rosbag2.storage_sqlite3 import ReaderSqlite3

if TYPE_CHECKING:
    from pathlib import Path

SQLITE_SCHEMA_V1 = """
CREATE TABLE topics(
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  type TEXT NOT NULL,
  serialization_format TEXT NOT NULL
);
CREATE TABLE messages(
  id INTEGER PRIMARY KEY,
  topic_id INTEGER NOT NULL,
  timestamp INTEGER NOT NULL,
  data BLOB NOT NULL
);
CREATE INDEX timestamp_idx ON messages (timestamp ASC);
"""

SQLITE_SCHEMA_V2 = """
CREATE TABLE topics(
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  type TEXT NOT NULL,
  serialization_format TEXT NOT NULL,
  offered_qos_profiles TEXT NOT NULL
);
CREATE TABLE messages(
  id INTEGER PRIMARY KEY,
  topic_id INTEGER NOT NULL,
  timestamp INTEGER NOT NULL,
  data BLOB NOT NULL
);
CREATE INDEX timestamp_idx ON messages (timestamp ASC);
"""

SQLITE_SCHEMA_V3 = """
CREATE TABLE schema(
  schema_version INTEGER PRIMARY KEY,
  ros_distro TEXT NOT NULL
);
CREATE TABLE metadata(
  id INTEGER PRIMARY KEY,
  metadata_version INTEGER NOT NULL,
  metadata TEXT NOT NULL
);
CREATE TABLE topics(
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  type TEXT NOT NULL,
  serialization_format TEXT NOT NULL,
  offered_qos_profiles TEXT NOT NULL
);
CREATE TABLE messages(
  id INTEGER PRIMARY KEY,
  topic_id INTEGER NOT NULL,
  timestamp INTEGER NOT NULL,
  data BLOB NOT NULL
);
CREATE INDEX timestamp_idx ON messages (timestamp ASC);
INSERT INTO schema(schema_version, ros_distro) VALUES (3, 'rosbags');
"""

SQLITE_SCHEMA_V4 = """
CREATE TABLE schema(
  schema_version INTEGER PRIMARY KEY,
  ros_distro TEXT NOT NULL
);
CREATE TABLE metadata(
  id INTEGER PRIMARY KEY,
  metadata_version INTEGER NOT NULL,
  metadata TEXT NOT NULL
);
CREATE TABLE topics(
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  type TEXT NOT NULL,
  serialization_format TEXT NOT NULL,
  offered_qos_profiles TEXT NOT NULL,
  type_description_hash TEXT NOT NULL
);
CREATE TABLE message_definitions(
  id INTEGER PRIMARY KEY,
  topic_type TEXT NOT NULL,
  encoding TEXT NOT NULL,
  encoded_message_definition TEXT NOT NULL,
  type_description_hash TEXT NOT NULL
);
CREATE TABLE messages(
  id INTEGER PRIMARY KEY,
  topic_id INTEGER NOT NULL,
  timestamp INTEGER NOT NULL,
  data BLOB NOT NULL
);
CREATE INDEX timestamp_idx ON messages (timestamp ASC);
INSERT INTO schema(schema_version, ros_distro) VALUES (4, 'rosbags');
"""


def test_detects_schema_version(tmp_path: Path) -> None:
    """Test schema version is detected."""
    for index, version in enumerate(
        [
            SQLITE_SCHEMA_V1,
            SQLITE_SCHEMA_V2,
            SQLITE_SCHEMA_V3,
            SQLITE_SCHEMA_V4,
        ],
    ):
        dbpath = tmp_path / 'db.db3'
        dbpath.unlink(missing_ok=True)
        con = sqlite3.connect(dbpath)
        con.executescript(version)
        con.close()
        reader = ReaderSqlite3([dbpath], [])
        reader.open()
        assert reader.schema == index + 1
        reader.close()


def test_type_definitions_are_read(tmp_path: Path) -> None:
    """Test type definitions are read."""
    dbpath = tmp_path / 'db.db3'
    con = sqlite3.connect(dbpath)
    con.executescript(SQLITE_SCHEMA_V4)
    with con:
        con.execute(
            'INSERT INTO message_definitions(topic_type, encoding,'
            ' encoded_message_definition, type_description_hash) VALUES (?, ?, ?, ?);',
            (
                'std_msgs/msg/Empty',
                'ros2msg',
                '',
                'RIHS01_20b625256f32d5dbc0d04fee44f43c41e51c70d3502f84b4a08e7a9c26a96312',
            ),
        )
    con.close()
    reader = ReaderSqlite3([dbpath], [])
    reader.open()
    assert reader.msgtypes
    reader.close()


def test_raises_on_closed_reader(tmp_path: Path) -> None:
    """Test type definitions are read."""
    dbpath = tmp_path / 'db.db3'
    con = sqlite3.connect(dbpath)
    con.executescript(SQLITE_SCHEMA_V4)
    con.close()

    reader = ReaderSqlite3([dbpath], [])

    with pytest.raises(ReaderError):
        reader.get_definitions()

    with pytest.raises(ReaderError):
        next(reader.messages())
