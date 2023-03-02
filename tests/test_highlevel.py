# Copyright 2020-2023  Ternaris.
# SPDX-License-Identifier: Apache-2.0
"""Reader tests."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest

from rosbags.highlevel import AnyReader, AnyReaderError
from rosbags.interfaces import Connection
from rosbags.rosbag1 import Writer as Writer1
from rosbags.rosbag2 import Writer as Writer2

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Sequence

HEADER = b'\x00\x01\x00\x00'


@pytest.fixture()
def bags1(tmp_path: Path) -> list[Path]:
    """Test data fixture."""
    paths = [
        tmp_path / 'ros1_1.bag',
        tmp_path / 'ros1_2.bag',
        tmp_path / 'ros1_3.bag',
        tmp_path / 'bad.bag',
    ]
    with (Writer1(paths[0])) as writer:
        topic1 = writer.add_connection('/topic1', 'std_msgs/msg/Int8')
        topic2 = writer.add_connection('/topic2', 'std_msgs/msg/Int16')
        writer.write(topic1, 1, b'\x01')
        writer.write(topic2, 2, b'\x02\x00')
        writer.write(topic1, 9, b'\x09')
    with (Writer1(paths[1])) as writer:
        topic1 = writer.add_connection('/topic1', 'std_msgs/msg/Int8')
        writer.write(topic1, 5, b'\x05')
    with (Writer1(paths[2])) as writer:
        topic2 = writer.add_connection('/topic2', 'std_msgs/msg/Int16')
        writer.write(topic2, 15, b'\x15\x00')

    paths[3].touch()

    return paths


@pytest.fixture()
def bags2(tmp_path: Path) -> list[Path]:
    """Test data fixture."""
    paths = [
        tmp_path / 'ros2_1',
        tmp_path / 'bad',
    ]
    with (Writer2(paths[0])) as writer:
        topic1 = writer.add_connection('/topic1', 'std_msgs/msg/Int8')
        topic2 = writer.add_connection('/topic2', 'std_msgs/msg/Int16')
        writer.write(topic1, 1, HEADER + b'\x01')
        writer.write(topic2, 2, HEADER + b'\x02\x00')
        writer.write(topic1, 9, HEADER + b'\x09')
        writer.write(topic1, 5, HEADER + b'\x05')
        writer.write(topic2, 15, HEADER + b'\x15\x00')

    paths[1].mkdir()
    (paths[1] / 'metadata.yaml').write_text(':')

    return paths


def test_anyreader1(bags1: Sequence[Path]) -> None:  # pylint: disable=redefined-outer-name
    """Test AnyReader on rosbag1."""
    # pylint: disable=too-many-statements
    with pytest.raises(AnyReaderError, match='at least one'):
        AnyReader([])

    with pytest.raises(AnyReaderError, match='missing'):
        AnyReader([bags1[0] / 'badname'])

    reader = AnyReader(bags1)
    with pytest.raises(AssertionError):
        assert reader.topics

    with pytest.raises(AssertionError):
        next(reader.messages())

    reader = AnyReader(bags1)
    with pytest.raises(AnyReaderError, match='seems to be empty'):
        reader.open()
    assert all(not x.bio for x in reader.readers)  # type: ignore[union-attr]

    with AnyReader(bags1[:3]) as reader:
        assert reader.duration == 15
        assert reader.start_time == 1
        assert reader.end_time == 16
        assert reader.message_count == 5
        assert list(reader.topics.keys()) == ['/topic1', '/topic2']
        assert len(reader.topics['/topic1'].connections) == 2
        assert reader.topics['/topic1'].msgcount == 3
        assert len(reader.topics['/topic2'].connections) == 2
        assert reader.topics['/topic2'].msgcount == 2

        gen = reader.messages()

        nxt = next(gen)
        assert nxt[0].topic == '/topic1'
        assert nxt[1:] == (1, b'\x01')
        msg = reader.deserialize(nxt[2], nxt[0].msgtype)
        assert msg.data == 1  # type: ignore
        nxt = next(gen)
        assert nxt[0].topic == '/topic2'
        assert nxt[1:] == (2, b'\x02\x00')
        msg = reader.deserialize(nxt[2], nxt[0].msgtype)
        assert msg.data == 2  # type: ignore
        nxt = next(gen)
        assert nxt[0].topic == '/topic1'
        assert nxt[1:] == (5, b'\x05')
        msg = reader.deserialize(nxt[2], nxt[0].msgtype)
        assert msg.data == 5  # type: ignore
        nxt = next(gen)
        assert nxt[0].topic == '/topic1'
        assert nxt[1:] == (9, b'\x09')
        msg = reader.deserialize(nxt[2], nxt[0].msgtype)
        assert msg.data == 9  # type: ignore
        nxt = next(gen)
        assert nxt[0].topic == '/topic2'
        assert nxt[1:] == (15, b'\x15\x00')
        msg = reader.deserialize(nxt[2], nxt[0].msgtype)
        assert msg.data == 21  # type: ignore
        with pytest.raises(StopIteration):
            next(gen)

        gen = reader.messages(connections=reader.topics['/topic1'].connections)
        nxt = next(gen)
        assert nxt[0].topic == '/topic1'
        nxt = next(gen)
        assert nxt[0].topic == '/topic1'
        nxt = next(gen)
        assert nxt[0].topic == '/topic1'
        with pytest.raises(StopIteration):
            next(gen)


def test_anyreader2(bags2: list[Path]) -> None:  # pylint: disable=redefined-outer-name
    """Test AnyReader on rosbag2."""
    # pylint: disable=too-many-statements
    with pytest.raises(AnyReaderError, match='multiple rosbag2'):
        AnyReader(bags2)

    with pytest.raises(AnyReaderError, match='YAML'):
        AnyReader([bags2[1]])

    with AnyReader([bags2[0]]) as reader:
        assert reader.duration == 15
        assert reader.start_time == 1
        assert reader.end_time == 16
        assert reader.message_count == 5
        assert list(reader.topics.keys()) == ['/topic1', '/topic2']
        assert len(reader.topics['/topic1'].connections) == 1
        assert reader.topics['/topic1'].msgcount == 3
        assert len(reader.topics['/topic2'].connections) == 1
        assert reader.topics['/topic2'].msgcount == 2

        gen = reader.messages()

        nxt = next(gen)
        assert nxt[0].topic == '/topic1'
        assert nxt[1:] == (1, HEADER + b'\x01')
        msg = reader.deserialize(nxt[2], nxt[0].msgtype)
        assert msg.data == 1  # type: ignore
        nxt = next(gen)
        assert nxt[0].topic == '/topic2'
        assert nxt[1:] == (2, HEADER + b'\x02\x00')
        msg = reader.deserialize(nxt[2], nxt[0].msgtype)
        assert msg.data == 2  # type: ignore
        nxt = next(gen)
        assert nxt[0].topic == '/topic1'
        assert nxt[1:] == (5, HEADER + b'\x05')
        msg = reader.deserialize(nxt[2], nxt[0].msgtype)
        assert msg.data == 5  # type: ignore
        nxt = next(gen)
        assert nxt[0].topic == '/topic1'
        assert nxt[1:] == (9, HEADER + b'\x09')
        msg = reader.deserialize(nxt[2], nxt[0].msgtype)
        assert msg.data == 9  # type: ignore
        nxt = next(gen)
        assert nxt[0].topic == '/topic2'
        assert nxt[1:] == (15, HEADER + b'\x15\x00')
        msg = reader.deserialize(nxt[2], nxt[0].msgtype)
        assert msg.data == 21  # type: ignore
        with pytest.raises(StopIteration):
            next(gen)

        gen = reader.messages(connections=reader.topics['/topic1'].connections)
        nxt = next(gen)
        assert nxt[0].topic == '/topic1'
        nxt = next(gen)
        assert nxt[0].topic == '/topic1'
        nxt = next(gen)
        assert nxt[0].topic == '/topic1'
        with pytest.raises(StopIteration):
            next(gen)


def test_anyreader2_autoregister(bags2: list[Path]) -> None:  # pylint: disable=redefined-outer-name
    """Test AnyReader on rosbag2."""

    class MockReader:
        """Mock reader."""

        # pylint: disable=too-few-public-methods

        def __init__(self, paths: list[Path]):
            """Initialize mock."""
            _ = paths
            self.metadata = {'storage_identifier': 'mcap'}
            self.connections = [
                Connection(
                    1,
                    '/foo',
                    'test_msg/msg/Foo',
                    'string foo',
                    'msg',
                    0,
                    None,  # type: ignore
                    self,
                ),
                Connection(
                    2,
                    '/bar',
                    'test_msg/msg/Bar',
                    'module test_msgs { module msg { struct Bar {string bar;}; }; };',
                    'idl',
                    0,
                    None,  # type: ignore
                    self,
                ),
                Connection(
                    3,
                    '/baz',
                    'test_msg/msg/Baz',
                    '',
                    '',
                    0,
                    None,  # type: ignore
                    self,
                ),
            ]

        def open(self) -> None:
            """Unused."""

    with patch('rosbags.highlevel.anyreader.Reader2', MockReader), \
         patch('rosbags.highlevel.anyreader.register_types') as mock_register_types:
        AnyReader([bags2[0]]).open()
    mock_register_types.assert_called_once()
    assert mock_register_types.call_args[0][0] == {
        'test_msg/msg/Foo': ([], [('foo', (1, 'string'))]),
        'test_msgs/msg/Bar': ([], [('bar', (1, 'string'))]),
    }
