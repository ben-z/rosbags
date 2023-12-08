# Copyright 2020-2023  Ternaris.
# SPDX-License-Identifier: Apache-2.0
"""Sqlite3 storage."""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

from rosbags.typesys.base import hash_rihs01
from rosbags.typesys.msg import get_types_from_msg

from .errors import ReaderError

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Any, Generator, Iterable, Optional

    from rosbags.interfaces import Connection


class ReaderSqlite3:
    """Sqlite3 storage reader."""

    def __init__(
        self,
        paths: Iterable[Path],
        connections: Iterable[Connection],
    ):
        """Set up storage reader.

        Args:
            paths: Paths of storage files.
            connections: List of connections.

        """
        self.paths = paths
        self.dbconns: list[sqlite3.Connection] = []
        self.schema = 0
        self.msgtypes: list[dict[str, str]] = []
        self.connections = connections

    def open(self) -> None:
        """Open rosbag2."""
        for path in self.paths:
            conn = sqlite3.connect(f'file:{path}?immutable=1', uri=True)
            conn.row_factory = lambda _, x: x
            cur = conn.cursor()
            cur.execute(
                'SELECT count(*) FROM sqlite_master '
                'WHERE type="table" AND name IN ("messages", "topics")',
            )
            if cur.fetchone()[0] != 2:
                raise ReaderError(f'Cannot open database {path} or database missing tables.')

            self.dbconns.append(conn)

        cur = self.dbconns[-1].cursor()
        if cur.execute('PRAGMA table_info(schema)').fetchall():
            schema, = cur.execute('SELECT schema_version FROM schema').fetchone()
        elif any(x[1] == 'offered_qos_profiles' for x in cur.execute('PRAGMA table_info(topics)')):
            schema = 2
        else:
            schema = 1

        if schema >= 4:
            msgtypes = [
                {
                    'name': x[0],
                    'encoding': x[1],
                    'msgdef': x[2],
                    'digest': x[3],
                } for x in cur.execute(
                    'SELECT topic_type, encoding, encoded_message_definition, type_description_hash'
                    ' FROM message_definitions ORDER BY id',
                )
            ]
            for typ in msgtypes:
                assert typ['encoding'] == 'ros2msg'
                types = get_types_from_msg(typ['msgdef'], typ['name'])

                class Store:  # pylint: disable=too-few-public-methods
                    FIELDDEFS = types

                assert typ['digest'] == hash_rihs01(
                    typ['name'],
                    Store,
                ), f'Failed to parse {typ["name"]}'
        else:
            msgtypes = []

        self.schema = schema
        self.msgtypes = msgtypes

    def close(self) -> None:
        """Close rosbag2."""
        assert self.dbconns
        for dbconn in self.dbconns:
            dbconn.close()
        self.dbconns.clear()

    def get_definitions(self) -> dict[str, tuple[str, str]]:
        """Get message definitions."""
        if not self.dbconns:
            raise ReaderError('Rosbag has not been opened.')
        return {x['name']: (x['encoding'][4:], x['msgdef']) for x in self.msgtypes}

    def messages(
        self,
        connections: Iterable[Connection] = (),
        start: Optional[int] = None,
        stop: Optional[int] = None,
    ) -> Generator[tuple[Connection, int, bytes], None, None]:
        """Read messages from bag.

        Args:
            connections: Iterable with connections to filter for. An empty
                iterable disables filtering on connections.
            start: Yield only messages at or after this timestamp (ns).
            stop: Yield only messages before this timestamp (ns).

        Yields:
            tuples of connection, timestamp (ns), and rawdata.

        Raises:
            ReaderError: Bag not open.

        """
        if not self.dbconns:
            raise ReaderError('Rosbag has not been opened.')

        query = [
            'SELECT topics.id,messages.timestamp,messages.data',
            'FROM messages JOIN topics ON messages.topic_id=topics.id',
        ]
        args: list[Any] = []
        clause = 'WHERE'

        if connections:
            topics = {x.topic for x in connections}
            query.append(f'{clause} topics.name IN ({",".join("?" for _ in topics)})')
            args += topics
            clause = 'AND'

        if start is not None:
            query.append(f'{clause} messages.timestamp >= ?')
            args.append(start)
            clause = 'AND'

        if stop is not None:
            query.append(f'{clause} messages.timestamp < ?')
            args.append(stop)
            clause = 'AND'

        query.append('ORDER BY timestamp')
        querystr = ' '.join(query)

        for conn in self.dbconns:
            cur = conn.cursor()
            cur.execute('SELECT name,id FROM topics')
            connmap: dict[int, Connection] = {
                row[1]: next((x for x in self.connections if x.topic == row[0]),
                             None)  # type: ignore
                for row in cur
            }

            cur.execute(querystr, args)

            for cid, timestamp, data in cur:
                yield connmap[cid], timestamp, data
