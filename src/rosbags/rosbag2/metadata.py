# Copyright 2020-2022  Ternaris.
# SPDX-License-Identifier: Apache-2.0
"""Rosbag2 metadata."""

from __future__ import annotations

from typing import TypedDict


class StartingTime(TypedDict):
    """Bag starting time."""

    nanoseconds_since_epoch: int


class Duration(TypedDict):
    """Bag starting time."""

    nanoseconds: int


class TopicMetadata(TypedDict):
    """Topic metadata."""

    name: str
    type: str
    serialization_format: str
    offered_qos_profiles: str


class TopicWithMessageCount(TypedDict):
    """Topic with message count."""

    message_count: int
    topic_metadata: TopicMetadata


class FileInformation(TypedDict):
    """Per file metadata."""

    path: str
    starting_time: StartingTime
    duration: Duration
    message_count: int


class Metadata(TypedDict):
    """Rosbag2 metadata file."""

    version: int
    storage_identifier: str
    relative_file_paths: list[str]
    starting_time: StartingTime
    duration: Duration
    message_count: int
    compression_format: str
    compression_mode: str
    topics_with_message_count: list[TopicWithMessageCount]
    files: list[FileInformation]
    custom_data: dict[str, str]
