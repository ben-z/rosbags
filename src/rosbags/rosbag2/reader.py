# Copyright 2020-2023  Ternaris.
# SPDX-License-Identifier: Apache-2.0
"""Rosbag2 reader."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Protocol

import zstandard
from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError

from rosbags.interfaces import Connection, ConnectionExtRosbag2, TopicInfo

from .errors import ReaderError
from .storage_mcap import ReaderMcap
from .storage_sqlite3 import ReaderSqlite3

if TYPE_CHECKING:
    from types import TracebackType
    from typing import Generator, Iterable, Literal, Optional, Type, Union

    from .metadata import FileInformation, Metadata


class StorageProtocol(Protocol):
    """Storage Protocol."""

    def __init__(self, paths: Iterable[Path], connections: Iterable[Connection]):
        """Initialize."""
        raise NotImplementedError  # pragma: no cover

    def open(self) -> None:
        """Open file."""
        raise NotImplementedError  # pragma: no cover

    def close(self) -> None:
        """Close file."""
        raise NotImplementedError  # pragma: no cover

    def get_definitions(self) -> dict[str, tuple[str, str]]:
        """Get message definitions."""
        raise NotImplementedError  # pragma: no cover

    def messages(
        self,
        connections: Iterable[Connection] = (),
        start: Optional[int] = None,
        stop: Optional[int] = None,
    ) -> Generator[tuple[Connection, int, bytes], None, None]:
        """Get messages from file."""
        raise NotImplementedError  # pragma: no cover


class Reader:
    """Reader for rosbag2 files.

    It implements all necessary features to access metadata and message
    streams.

    Version history:

        - Version 1: Initial format.
        - Version 2: Changed field sizes in C++ implementation.
        - Version 3: Added compression.
        - Version 4: Added QoS metadata to topics, changed relative file paths
        - Version 5: Added per file metadata
        - Version 6: Added custom_data dict to metadata

    """

    # pylint: disable=too-many-instance-attributes

    STORAGE_PLUGINS: dict[str, Type[StorageProtocol]] = {
        'mcap': ReaderMcap,
        'sqlite3': ReaderSqlite3,
    }

    def __init__(self, path: Union[Path, str]):
        """Open rosbag and check metadata.

        Args:
            path: Filesystem path to bag.

        Raises:
            ReaderError: Bag not readable or bag metadata.

        """
        path = Path(path)
        yamlpath = path / 'metadata.yaml'
        self.path = path
        try:
            yaml = YAML(typ='safe')
            dct = yaml.load(yamlpath.read_text())
        except OSError as err:
            raise ReaderError(f'Could not read metadata at {yamlpath}: {err}.') from None
        except YAMLError as exc:
            raise ReaderError(f'Could not load YAML from {yamlpath}: {exc}') from None

        try:
            self.metadata: Metadata = dct['rosbag2_bagfile_information']
            if (ver := self.metadata['version']) > 6:
                raise ReaderError(f'Rosbag2 version {ver} not supported; please report issue.')
            if (storageid := self.metadata['storage_identifier']) not in self.STORAGE_PLUGINS:
                raise ReaderError(
                    f'Storage plugin {storageid!r} not supported; please report issue.',
                )

            self.paths = [path / Path(x).name for x in self.metadata['relative_file_paths']]
            if missing := [x for x in self.paths if not x.exists()]:
                raise ReaderError(f'Some database files are missing: {[str(x) for x in missing]!r}')

            self.connections = [
                Connection(
                    id=idx + 1,
                    topic=x['topic_metadata']['name'],
                    msgtype=x['topic_metadata']['type'],
                    msgdef='',
                    md5sum='',
                    msgcount=x['message_count'],
                    ext=ConnectionExtRosbag2(
                        serialization_format=x['topic_metadata']['serialization_format'],
                        offered_qos_profiles=x['topic_metadata'].get('offered_qos_profiles', ''),
                    ),
                    owner=self,
                ) for idx, x in enumerate(self.metadata['topics_with_message_count'])
            ]
            noncdr = {
                fmt for x in self.connections if isinstance(x.ext, ConnectionExtRosbag2)
                if (fmt := x.ext.serialization_format) != 'cdr'
            }
            if noncdr:
                raise ReaderError(f'Serialization format {noncdr!r} is not supported.')

            if self.compression_mode and (cfmt := self.compression_format) != 'zstd':
                raise ReaderError(f'Compression format {cfmt!r} is not supported.')

            self.files: list[FileInformation] = self.metadata.get('files', [])[:]
            self.custom_data: dict[str, str] = self.metadata.get('custom_data', {})

            self.tmpdir: Optional[TemporaryDirectory[str]] = None
            self.storage: Optional[StorageProtocol] = None
        except KeyError as exc:
            raise ReaderError(f'A metadata key is missing {exc!r}.') from None

    @property
    def duration(self) -> int:
        """Duration in nanoseconds between earliest and latest messages."""
        nsecs: int = self.metadata['duration']['nanoseconds']
        return nsecs + 1 if self.message_count else 0

    @property
    def start_time(self) -> int:
        """Timestamp in nanoseconds of the earliest message."""
        nsecs: int = self.metadata['starting_time']['nanoseconds_since_epoch']
        return nsecs if self.message_count else 2**63 - 1

    @property
    def end_time(self) -> int:
        """Timestamp in nanoseconds after the latest message."""
        return self.start_time + self.duration if self.message_count else 0

    @property
    def message_count(self) -> int:
        """Total message count."""
        return self.metadata['message_count']

    @property
    def compression_format(self) -> Optional[str]:
        """Compression format."""
        return self.metadata.get('compression_format', None) or None

    @property
    def compression_mode(self) -> Optional[str]:
        """Compression mode."""
        mode = self.metadata.get('compression_mode', '').lower()
        return mode if mode != 'none' else None

    @property
    def topics(self) -> dict[str, TopicInfo]:
        """Topic information."""
        return {x.topic: TopicInfo(x.msgtype, x.msgdef, x.msgcount, [x]) for x in self.connections}

    def open(self) -> None:
        """Open rosbag2."""
        storage_paths = []
        if self.compression_mode == 'file':
            self.tmpdir = TemporaryDirectory()  # pylint: disable=consider-using-with
            tmpdir = self.tmpdir.name
            decomp = zstandard.ZstdDecompressor()
            for path in self.paths:
                storage_file = Path(tmpdir, path.stem)
                with path.open('rb') as infile, storage_file.open('wb') as outfile:
                    decomp.copy_stream(infile, outfile)
                storage_paths.append(storage_file)
        else:
            storage_paths = self.paths[:]

        self.storage = self.STORAGE_PLUGINS[self.metadata['storage_identifier']](
            storage_paths,
            self.connections,
        )
        self.storage.open()
        definitions = self.storage.get_definitions()
        for idx, conn in enumerate(self.connections):
            if desc := definitions.get(conn.msgtype):
                self.connections[idx] = Connection(
                    id=conn.id,
                    topic=conn.topic,
                    msgtype=conn.msgtype,
                    msgdef=desc[1],
                    md5sum=desc[0],
                    msgcount=conn.msgcount,
                    ext=conn.ext,
                    owner=conn.owner,
                )

    def close(self) -> None:
        """Close rosbag2."""
        assert self.storage
        self.storage.close()
        self.storage = None
        if self.tmpdir:
            self.tmpdir.cleanup()
            self.tmpdir = None

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
            ReaderError: If reader was not opened.

        """
        if not self.storage:
            raise ReaderError('Rosbag is not open.')

        if self.compression_mode == 'message':
            decomp = zstandard.ZstdDecompressor().decompress
            for connection, timestamp, data in self.storage.messages(connections, start, stop):
                yield connection, timestamp, decomp(data)
        else:
            yield from self.storage.messages(connections, start, stop)

    def __enter__(self) -> Reader:
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
