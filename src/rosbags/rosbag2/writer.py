# Copyright 2020-2023  Ternaris.
# SPDX-License-Identifier: Apache-2.0
"""Rosbag2 writer."""

from __future__ import annotations

import sqlite3
from enum import IntEnum, auto
from pathlib import Path
from typing import TYPE_CHECKING

import zstandard
from ruamel.yaml import YAML

from rosbags.interfaces import Connection, ConnectionExtRosbag2

if TYPE_CHECKING:
    from types import TracebackType
    from typing import Any, Literal, Optional, Type, Union

    from .metadata import Metadata


class WriterError(Exception):
    """Writer Error."""


class Writer:  # pylint: disable=too-many-instance-attributes
    """Rosbag2 writer.

    This class implements writing of rosbag2 files in version 4. It should be
    used as a contextmanager.

    """

    SQLITE_SCHEMA = """
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

    class CompressionMode(IntEnum):
        """Compession modes."""

        NONE = auto()
        FILE = auto()
        MESSAGE = auto()

    class CompressionFormat(IntEnum):
        """Compession formats."""

        ZSTD = auto()

    def __init__(self, path: Union[Path, str]):
        """Initialize writer.

        Args:
            path: Filesystem path to bag.

        Raises:
            WriterError: Target path exisits already, Writer can only create new rosbags.

        """
        path = Path(path)
        self.path = path
        if path.exists():
            raise WriterError(f'{path} exists already, not overwriting.')
        self.metapath = path / 'metadata.yaml'
        self.dbpath = path / f'{path.name}.db3'
        self.compression_mode = ''
        self.compression_format = ''
        self.compressor: Optional[zstandard.ZstdCompressor] = None
        self.connections: list[Connection] = []
        self.counts: dict[int, int] = {}
        self.conn: Optional[sqlite3.Connection] = None
        self.cursor: Optional[sqlite3.Cursor] = None
        self.custom_data: dict[str, str] = {}

    def set_compression(self, mode: CompressionMode, fmt: CompressionFormat) -> None:
        """Enable compression on bag.

        This function has to be called before opening.

        Args:
            mode: Compression mode to use, either 'file' or 'message'.
            fmt: Compressor to use, currently only 'zstd'.

        Raises:
            WriterError: Bag already open.

        """
        if self.conn:
            raise WriterError(f'Cannot set compression, bag {self.path} already open.')
        if mode == self.CompressionMode.NONE:
            return
        self.compression_mode = mode.name.lower()
        self.compression_format = fmt.name.lower()
        self.compressor = zstandard.ZstdCompressor()

    def set_custom_data(self, key: str, value: str) -> None:
        """Set key value pair in custom_data.

        Args:
            key: Key to set.
            value: Value to set.

        Raises:
            WriterError: If value has incorrect type.

        """
        if not isinstance(value, str):
            raise WriterError(f'Cannot set non-string value {value!r} in custom_data.')
        self.custom_data[key] = value

    def open(self) -> None:
        """Open rosbag2 for writing.

        Create base directory and open database connection.

        """
        try:
            self.path.mkdir(mode=0o755, parents=True)
        except FileExistsError:
            raise WriterError(f'{self.path} exists already, not overwriting.') from None

        self.conn = sqlite3.connect(f'file:{self.dbpath}', uri=True)
        self.conn.executescript(self.SQLITE_SCHEMA)
        self.cursor = self.conn.cursor()

    def add_connection(
        self,
        topic: str,
        msgtype: str,
        serialization_format: str = 'cdr',
        offered_qos_profiles: str = '',
    ) -> Connection:
        """Add a connection.

        This function can only be called after opening a bag.

        Args:
            topic: Topic name.
            msgtype: Message type.
            serialization_format: Serialization format.
            offered_qos_profiles: QOS Profile.

        Returns:
            Connection object.

        Raises:
            WriterError: Bag not open or topic previously registered.

        """
        if not self.cursor:
            raise WriterError('Bag was not opened.')

        connection = Connection(
            id=len(self.connections) + 1,
            topic=topic,
            msgtype=msgtype,
            msgdef='',
            digest='',
            msgcount=0,
            ext=ConnectionExtRosbag2(
                serialization_format=serialization_format,
                offered_qos_profiles=offered_qos_profiles,
            ),
            owner=self,
        )
        for conn in self.connections:
            if (
                conn.topic == connection.topic and conn.msgtype == connection.msgtype and
                conn.ext == connection.ext
            ):
                raise WriterError(f'Connection can only be added once: {connection!r}.')

        self.connections.append(connection)
        self.counts[connection.id] = 0
        meta = (connection.id, topic, msgtype, serialization_format, offered_qos_profiles)
        self.cursor.execute('INSERT INTO topics VALUES(?, ?, ?, ?, ?)', meta)
        return connection

    def write(self, connection: Connection, timestamp: int, data: bytes) -> None:
        """Write message to rosbag2.

        Args:
            connection: Connection to write message to.
            timestamp: Message timestamp (ns).
            data: Serialized message data.

        Raises:
            WriterError: Bag not open or topic not registered.

        """
        if not self.cursor:
            raise WriterError('Bag was not opened.')
        if connection not in self.connections:
            raise WriterError(f'Tried to write to unknown connection {connection!r}.')

        if self.compression_mode == 'message':
            assert self.compressor
            data = self.compressor.compress(data)

        self.cursor.execute(
            'INSERT INTO messages (topic_id, timestamp, data) VALUES(?, ?, ?)',
            (connection.id, timestamp, data),
        )
        self.counts[connection.id] += 1

    def close(self) -> None:
        """Close rosbag2 after writing.

        Closes open database transactions and writes metadata.yaml.

        """
        assert self.cursor
        assert self.conn
        self.cursor.close()
        self.cursor = None

        duration, start, count = self.conn.execute(
            'SELECT max(timestamp) - min(timestamp), min(timestamp), count(*) FROM messages',
        ).fetchone()

        self.conn.commit()
        self.conn.execute('PRAGMA optimize')
        self.conn.close()

        if self.compression_mode == 'file':
            assert self.compressor
            src = self.dbpath
            self.dbpath = src.with_suffix(f'.db3.{self.compression_format}')
            with src.open('rb') as infile, self.dbpath.open('wb') as outfile:
                self.compressor.copy_stream(infile, outfile)
            src.unlink()

        metadata: dict[str, Metadata] = {
            'rosbag2_bagfile_information': {
                'version': 6,
                'storage_identifier': 'sqlite3',
                'relative_file_paths': [self.dbpath.name],
                'duration': {
                    'nanoseconds': duration,
                },
                'starting_time': {
                    'nanoseconds_since_epoch': start,
                },
                'message_count': count,
                'topics_with_message_count': [
                    {
                        'topic_metadata': {
                            'name': x.topic,
                            'type': x.msgtype,
                            'serialization_format': x.ext.serialization_format,
                            'offered_qos_profiles': x.ext.offered_qos_profiles,
                        },
                        'message_count': self.counts[x.id],
                    } for x in self.connections if isinstance(x.ext, ConnectionExtRosbag2)
                ],
                'compression_format': self.compression_format,
                'compression_mode': self.compression_mode,
                'files': [
                    {
                        'path': self.dbpath.name,
                        'starting_time': {
                            'nanoseconds_since_epoch': start,
                        },
                        'duration': {
                            'nanoseconds': duration,
                        },
                        'message_count': count,
                    },
                ],
                'custom_data': self.custom_data,
            },
        }
        with self.metapath.open('w') as metafile:
            yaml = YAML(typ='safe')
            yaml.default_flow_style = False
            yaml.dump(metadata, metafile)

    def __enter__(self) -> Writer:
        """Open rosbag2 when entering contextmanager."""
        self.open()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> Literal[False]:
        """Close rosbag2 when exiting contextmanager."""
        self.close()
        return False
