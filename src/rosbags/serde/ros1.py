# Copyright 2020-2022  Ternaris.
# SPDX-License-Identifier: Apache-2.0
"""Code generators for ROS1.

`ROS1`_ uses a serialization format. This module supports fast byte-level
conversion of ROS1 to CDR.

.. _ROS1: http://wiki.ros.org/ROS/Technical%20Overview

"""

from __future__ import annotations

import sys
from itertools import tee
from typing import TYPE_CHECKING, Iterator, cast

from .typing import Field
from .utils import SIZEMAP, Valtype, align, align_after, compile_lines

if TYPE_CHECKING:
    from typing import Union

    from .typing import Bitcvt, BitcvtSize, CDRDeser, CDRSer, CDRSerSize


def generate_ros1_to_cdr(
    fields: list[Field],
    typename: str,
    copy: bool,
) -> Union[Bitcvt, BitcvtSize]:
    """Generate ROS1 to CDR conversion function.

    Args:
        fields: Fields of message.
        typename: Message type name.
        copy: Generate conversion or sizing function.

    Returns:
        ROS1 to CDR conversion function.

    """
    # pylint: disable=too-many-branches,too-many-locals,too-many-nested-blocks,too-many-statements
    aligned = 8
    iterators = tee([*fields, None])
    icurr = cast(Iterator[Field], iterators[0])
    inext = iterators[1]
    next(inext)
    funcname = 'ros1_to_cdr' if copy else 'getsize_ros1_to_cdr'
    lines = [
        'import sys',
        'import numpy',
        'from rosbags.serde.messages import SerdeError, get_msgdef',
        'from rosbags.serde.primitives import pack_int32_le',
        'from rosbags.serde.primitives import unpack_int32_le',
        f'def {funcname}(input, ipos, output, opos, typestore):',
    ]

    if typename == 'std_msgs/msg/Header':
        lines.append('  ipos += 4')

    for fcurr, fnext in zip(icurr, inext):
        _, desc = fcurr

        if desc.valtype == Valtype.MESSAGE:
            lines.append(f'  func = get_msgdef("{desc.args.name}", typestore).{funcname}')
            lines.append('  ipos, opos = func(input, ipos, output, opos, typestore)')
            aligned = align_after(desc)

        elif desc.valtype == Valtype.BASE:
            if desc.args == 'string':
                lines.append('  length = unpack_int32_le(input, ipos)[0] + 1')
                if copy:
                    lines.append('  pack_int32_le(output, opos, length)')
                lines.append('  ipos += 4')
                lines.append('  opos += 4')
                if copy:
                    lines.append('  output[opos:opos + length - 1] = input[ipos:ipos + length - 1]')
                lines.append('  ipos += length - 1')
                lines.append('  opos += length')
                aligned = 1
            else:
                size = SIZEMAP[desc.args]
                if copy:
                    lines.append(f'  output[opos:opos + {size}] = input[ipos:ipos + {size}]')
                lines.append(f'  ipos += {size}')
                lines.append(f'  opos += {size}')
                aligned = size

        elif desc.valtype == Valtype.ARRAY:
            subdesc, length = desc.args

            if subdesc.valtype == Valtype.BASE:
                if subdesc.args == 'string':
                    for _ in range(length):
                        lines.append('  opos = (opos + 4 - 1) & -4')
                        lines.append('  length = unpack_int32_le(input, ipos)[0] + 1')
                        if copy:
                            lines.append('  pack_int32_le(output, opos, length)')
                        lines.append('  ipos += 4')
                        lines.append('  opos += 4')
                        if copy:
                            lines.append(
                                '  output[opos:opos + length - 1] = input[ipos:ipos + length - 1]',
                            )
                        lines.append('  ipos += length - 1')
                        lines.append('  opos += length')
                    aligned = 1
                else:
                    size = length * SIZEMAP[subdesc.args]
                    if copy:
                        lines.append(f'  output[opos:opos + {size}] = input[ipos:ipos + {size}]')
                    lines.append(f'  ipos += {size}')
                    lines.append(f'  opos += {size}')
                    aligned = SIZEMAP[subdesc.args]

            if subdesc.valtype == Valtype.MESSAGE:
                anext_before = align(subdesc)
                anext_after = align_after(subdesc)

                lines.append(f'  func = get_msgdef("{subdesc.args.name}", typestore).{funcname}')
                for _ in range(length):
                    if anext_before > anext_after:
                        lines.append(f'  opos = (opos + {anext_before} - 1) & -{anext_before}')
                    lines.append('  ipos, opos = func(input, ipos, output, opos, typestore)')
                aligned = anext_after
        else:
            assert desc.valtype == Valtype.SEQUENCE
            lines.append('  size = unpack_int32_le(input, ipos)[0]')
            if copy:
                lines.append('  pack_int32_le(output, opos, size)')
            lines.append('  ipos += 4')
            lines.append('  opos += 4')
            subdesc = desc.args[0]
            aligned = 4

            if subdesc.valtype == Valtype.BASE:
                if subdesc.args == 'string':
                    lines.append('  for _ in range(size):')
                    lines.append('    length = unpack_int32_le(input, ipos)[0] + 1')
                    lines.append('    opos = (opos + 4 - 1) & -4')
                    if copy:
                        lines.append('    pack_int32_le(output, opos, length)')
                    lines.append('    ipos += 4')
                    lines.append('    opos += 4')
                    if copy:
                        lines.append(
                            '    output[opos:opos + length - 1] = input[ipos:ipos + length - 1]',
                        )
                    lines.append('    ipos += length - 1')
                    lines.append('    opos += length')
                    aligned = 1
                else:
                    if aligned < (anext_before := align(subdesc)):
                        lines.append('  if size:')
                        lines.append(f'    opos = (opos + {anext_before} - 1) & -{anext_before}')
                    lines.append(f'  length = size * {SIZEMAP[subdesc.args]}')
                    if copy:
                        lines.append('  output[opos:opos + length] = input[ipos:ipos + length]')
                    lines.append('  ipos += length')
                    lines.append('  opos += length')
                    aligned = anext_before

            else:
                assert subdesc.valtype == Valtype.MESSAGE
                anext_before = align(subdesc)
                lines.append(f'  func = get_msgdef("{subdesc.args.name}", typestore).{funcname}')
                lines.append('  for _ in range(size):')
                lines.append(f'    opos = (opos + {anext_before} - 1) & -{anext_before}')
                lines.append('    ipos, opos = func(input, ipos, output, opos, typestore)')
                aligned = align_after(subdesc)

            aligned = min([aligned, 4])

        if fnext and aligned < (anext_before := align(fnext.descriptor)):
            lines.append(f'  opos = (opos + {anext_before} - 1) & -{anext_before}')
            aligned = anext_before

    lines.append('  return ipos, opos')
    return getattr(compile_lines(lines), funcname)  # type: ignore


def generate_cdr_to_ros1(
    fields: list[Field],
    typename: str,
    copy: bool,
) -> Union[Bitcvt, BitcvtSize]:
    """Generate CDR to ROS1 conversion function.

    Args:
        fields: Fields of message.
        typename: Message type name.
        copy: Generate conversion or sizing function.

    Returns:
        CDR to ROS1 conversion function.

    """
    # pylint: disable=too-many-branches,too-many-locals,too-many-nested-blocks,too-many-statements
    aligned = 8
    iterators = tee([*fields, None])
    icurr = cast(Iterator[Field], iterators[0])
    inext = iterators[1]
    next(inext)
    funcname = 'cdr_to_ros1' if copy else 'getsize_cdr_to_ros1'
    lines = [
        'import sys',
        'import numpy',
        'from rosbags.serde.messages import SerdeError, get_msgdef',
        'from rosbags.serde.primitives import pack_int32_le',
        'from rosbags.serde.primitives import unpack_int32_le',
        f'def {funcname}(input, ipos, output, opos, typestore):',
    ]

    if typename == 'std_msgs/msg/Header':
        lines.append('  opos += 4')

    for fcurr, fnext in zip(icurr, inext):
        _, desc = fcurr

        if desc.valtype == Valtype.MESSAGE:
            lines.append(f'  func = get_msgdef("{desc.args.name}", typestore).{funcname}')
            lines.append('  ipos, opos = func(input, ipos, output, opos, typestore)')
            aligned = align_after(desc)

        elif desc.valtype == Valtype.BASE:
            if desc.args == 'string':
                lines.append('  length = unpack_int32_le(input, ipos)[0] - 1')
                if copy:
                    lines.append('  pack_int32_le(output, opos, length)')
                lines.append('  ipos += 4')
                lines.append('  opos += 4')
                if copy:
                    lines.append('  output[opos:opos + length] = input[ipos:ipos + length]')
                lines.append('  ipos += length + 1')
                lines.append('  opos += length')
                aligned = 1
            else:
                size = SIZEMAP[desc.args]
                if copy:
                    lines.append(f'  output[opos:opos + {size}] = input[ipos:ipos + {size}]')
                lines.append(f'  ipos += {size}')
                lines.append(f'  opos += {size}')
                aligned = size

        elif desc.valtype == Valtype.ARRAY:
            subdesc, length = desc.args

            if subdesc.valtype == Valtype.BASE:
                if subdesc.args == 'string':
                    for _ in range(length):
                        lines.append('  ipos = (ipos + 4 - 1) & -4')
                        lines.append('  length = unpack_int32_le(input, ipos)[0] - 1')
                        if copy:
                            lines.append('  pack_int32_le(output, opos, length)')
                        lines.append('  ipos += 4')
                        lines.append('  opos += 4')
                        if copy:
                            lines.append(
                                '  output[opos:opos + length] = input[ipos:ipos + length]',
                            )
                        lines.append('  ipos += length + 1')
                        lines.append('  opos += length')
                    aligned = 1
                else:
                    size = length * SIZEMAP[subdesc.args]
                    if copy:
                        lines.append(f'  output[opos:opos + {size}] = input[ipos:ipos + {size}]')
                    lines.append(f'  ipos += {size}')
                    lines.append(f'  opos += {size}')
                    aligned = SIZEMAP[subdesc.args]

            if subdesc.valtype == Valtype.MESSAGE:
                anext_before = align(subdesc)
                anext_after = align_after(subdesc)

                lines.append(f'  func = get_msgdef("{subdesc.args.name}", typestore).{funcname}')
                for _ in range(length):
                    if anext_before > anext_after:
                        lines.append(f'  ipos = (ipos + {anext_before} - 1) & -{anext_before}')
                    lines.append('  ipos, opos = func(input, ipos, output, opos, typestore)')
                aligned = anext_after
        else:
            assert desc.valtype == Valtype.SEQUENCE
            lines.append('  size = unpack_int32_le(input, ipos)[0]')
            if copy:
                lines.append('  pack_int32_le(output, opos, size)')
            lines.append('  ipos += 4')
            lines.append('  opos += 4')
            subdesc = desc.args[0]
            aligned = 4

            if subdesc.valtype == Valtype.BASE:
                if subdesc.args == 'string':
                    lines.append('  for _ in range(size):')
                    lines.append('    ipos = (ipos + 4 - 1) & -4')
                    lines.append('    length = unpack_int32_le(input, ipos)[0] - 1')
                    if copy:
                        lines.append('    pack_int32_le(output, opos, length)')
                    lines.append('    ipos += 4')
                    lines.append('    opos += 4')
                    if copy:
                        lines.append('    output[opos:opos + length] = input[ipos:ipos + length]')
                    lines.append('    ipos += length + 1')
                    lines.append('    opos += length')
                    aligned = 1
                else:
                    if aligned < (anext_before := align(subdesc)):
                        lines.append('  if size:')
                        lines.append(f'    ipos = (ipos + {anext_before} - 1) & -{anext_before}')
                    lines.append(f'  length = size * {SIZEMAP[subdesc.args]}')
                    if copy:
                        lines.append('  output[opos:opos + length] = input[ipos:ipos + length]')
                    lines.append('  ipos += length')
                    lines.append('  opos += length')
                    aligned = anext_before

            else:
                assert subdesc.valtype == Valtype.MESSAGE
                anext_before = align(subdesc)
                lines.append(f'  func = get_msgdef("{subdesc.args.name}", typestore).{funcname}')
                lines.append('  for _ in range(size):')
                lines.append(f'    ipos = (ipos + {anext_before} - 1) & -{anext_before}')
                lines.append('    ipos, opos = func(input, ipos, output, opos, typestore)')
                aligned = align_after(subdesc)

            aligned = min([aligned, 4])

        if fnext and aligned < (anext_before := align(fnext.descriptor)):
            lines.append(f'  ipos = (ipos + {anext_before} - 1) & -{anext_before}')
            aligned = anext_before

    lines.append('  return ipos, opos')
    return getattr(compile_lines(lines), funcname)  # type: ignore


def generate_getsize_ros1(fields: list[Field], typename: str) -> tuple[CDRSerSize, int]:
    """Generate ros1 size calculation function.

    Args:
        fields: Fields of message.
        typename: Message type name.

    Returns:
        Size calculation function and static size.

    """
    # pylint: disable=too-many-branches,too-many-statements
    size = 0
    is_stat = True

    lines = [
        'import sys',
        'from rosbags.serde.messages import get_msgdef',
        'def getsize_ros1(pos, message, typestore):',
    ]

    if typename == 'std_msgs/msg/Header':
        lines.append('  pos += 4')

    for fcurr in fields:
        fieldname, desc = fcurr

        if desc.valtype == Valtype.MESSAGE:
            if desc.args.size_ros1:
                lines.append(f'  pos += {desc.args.size_ros1}')
                size += desc.args.size_ros1
            else:
                lines.append(f'  func = get_msgdef("{desc.args.name}", typestore).getsize_ros1')
                lines.append(f'  pos = func(pos, message.{fieldname}, typestore)')
                is_stat = False

        elif desc.valtype == Valtype.BASE:
            if desc.args == 'string':
                lines.append(f'  pos += 4 + len(message.{fieldname}.encode())')
                is_stat = False
            else:
                lines.append(f'  pos += {SIZEMAP[desc.args]}')
                size += SIZEMAP[desc.args]

        elif desc.valtype == Valtype.ARRAY:
            subdesc, length = desc.args

            if subdesc.valtype == Valtype.BASE:
                if subdesc.args == 'string':
                    lines.append(f'  val = message.{fieldname}')
                    for idx in range(length):
                        lines.append(f'  pos += 4 + len(val[{idx}].encode())')
                    is_stat = False
                else:
                    lines.append(f'  pos += {length * SIZEMAP[subdesc.args]}')
                    size += length * SIZEMAP[subdesc.args]

            else:
                assert subdesc.valtype == Valtype.MESSAGE
                if subdesc.args.size_ros1:
                    for _ in range(length):
                        lines.append(f'  pos += {subdesc.args.size_ros1}')
                        size += subdesc.args.size_ros1
                else:
                    lines.append(
                        f'  func = get_msgdef("{subdesc.args.name}", typestore).getsize_ros1',
                    )
                    lines.append(f'  val = message.{fieldname}')
                    for idx in range(length):
                        lines.append(f'  pos = func(pos, val[{idx}], typestore)')
                    is_stat = False
        else:
            assert desc.valtype == Valtype.SEQUENCE
            lines.append('  pos += 4')
            subdesc = desc.args[0]
            if subdesc.valtype == Valtype.BASE:
                if subdesc.args == 'string':
                    lines.append(f'  for val in message.{fieldname}:')
                    lines.append('    pos += 4 + len(val.encode())')
                else:
                    lines.append(f'  pos += len(message.{fieldname}) * {SIZEMAP[subdesc.args]}')

            else:
                assert subdesc.valtype == Valtype.MESSAGE
                lines.append(f'  val = message.{fieldname}')
                if subdesc.args.size_ros1:
                    lines.append(f'  pos += {subdesc.args.size_ros1} * len(val)')

                else:
                    lines.append(
                        f'  func = get_msgdef("{subdesc.args.name}", typestore).getsize_ros1',
                    )
                    lines.append('  for item in val:')
                    lines.append('    pos = func(pos, item, typestore)')

            is_stat = False
    lines.append('  return pos')
    return compile_lines(lines).getsize_ros1, is_stat * size


def generate_serialize_ros1(fields: list[Field], typename: str) -> CDRSer:
    """Generate ros1 serialization function.

    Args:
        fields: Fields of message.
        typename: Message type name.

    Returns:
        Serializer function.

    """
    # pylint: disable=too-many-branches,too-many-statements
    lines = [
        'import sys',
        'import numpy',
        'from rosbags.serde.messages import SerdeError, get_msgdef',
        'from rosbags.serde.primitives import pack_bool_le',
        'from rosbags.serde.primitives import pack_int8_le',
        'from rosbags.serde.primitives import pack_int16_le',
        'from rosbags.serde.primitives import pack_int32_le',
        'from rosbags.serde.primitives import pack_int64_le',
        'from rosbags.serde.primitives import pack_uint8_le',
        'from rosbags.serde.primitives import pack_uint16_le',
        'from rosbags.serde.primitives import pack_uint32_le',
        'from rosbags.serde.primitives import pack_uint64_le',
        'from rosbags.serde.primitives import pack_float32_le',
        'from rosbags.serde.primitives import pack_float64_le',
        'def serialize_ros1(rawdata, pos, message, typestore):',
    ]

    if typename == 'std_msgs/msg/Header':
        lines.append('  pos += 4')

    be_syms = ('>',) if sys.byteorder == 'little' else ('=', '>')

    for fcurr in fields:
        fieldname, desc = fcurr

        lines.append(f'  val = message.{fieldname}')
        if desc.valtype == Valtype.MESSAGE:
            name = desc.args.name
            lines.append(f'  func = get_msgdef("{name}", typestore).serialize_ros1')
            lines.append('  pos = func(rawdata, pos, val, typestore)')

        elif desc.valtype == Valtype.BASE:
            if desc.args == 'string':
                lines.append('  bval = memoryview(val.encode())')
                lines.append('  length = len(bval)')
                lines.append('  pack_int32_le(rawdata, pos, length)')
                lines.append('  pos += 4')
                lines.append('  rawdata[pos:pos + length] = bval')
                lines.append('  pos += length')
            else:
                lines.append(f'  pack_{desc.args}_le(rawdata, pos, val)')
                lines.append(f'  pos += {SIZEMAP[desc.args]}')

        elif desc.valtype == Valtype.ARRAY:
            subdesc, length = desc.args
            lines.append(f'  if len(val) != {length}:')
            lines.append('    raise SerdeError(\'Unexpected array length\')')

            if subdesc.valtype == Valtype.BASE:
                if subdesc.args == 'string':
                    for idx in range(length):
                        lines.append(f'  bval = memoryview(val[{idx}].encode())')
                        lines.append('  length = len(bval)')
                        lines.append('  pack_int32_le(rawdata, pos, length)')
                        lines.append('  pos += 4')
                        lines.append('  rawdata[pos:pos + length] = bval')
                        lines.append('  pos += length')
                else:
                    lines.append(f'  if val.dtype.byteorder in {be_syms}:')
                    lines.append('    val = val.byteswap()')
                    size = length * SIZEMAP[subdesc.args]
                    lines.append(f'  rawdata[pos:pos + {size}] = val.view(numpy.uint8)')
                    lines.append(f'  pos += {size}')

            else:
                assert subdesc.valtype == Valtype.MESSAGE
                name = subdesc.args.name
                lines.append(f'  func = get_msgdef("{name}", typestore).serialize_ros1')
                for idx in range(length):
                    lines.append(f'  pos = func(rawdata, pos, val[{idx}], typestore)')
        else:
            assert desc.valtype == Valtype.SEQUENCE
            lines.append('  pack_int32_le(rawdata, pos, len(val))')
            lines.append('  pos += 4')
            subdesc = desc.args[0]

            if subdesc.valtype == Valtype.BASE:
                if subdesc.args == 'string':
                    lines.append('  for item in val:')
                    lines.append('    bval = memoryview(item.encode())')
                    lines.append('    length = len(bval)')
                    lines.append('    pack_int32_le(rawdata, pos, length)')
                    lines.append('    pos += 4')
                    lines.append('    rawdata[pos:pos + length] = bval')
                    lines.append('    pos += length')
                else:
                    lines.append(f'  size = len(val) * {SIZEMAP[subdesc.args]}')
                    lines.append(f'  if val.dtype.byteorder in {be_syms}:')
                    lines.append('    val = val.byteswap()')
                    lines.append('  rawdata[pos:pos + size] = val.view(numpy.uint8)')
                    lines.append('  pos += size')

            if subdesc.valtype == Valtype.MESSAGE:
                name = subdesc.args.name
                lines.append(f'  func = get_msgdef("{name}", typestore).serialize_ros1')
                lines.append('  for item in val:')
                lines.append('    pos = func(rawdata, pos, item, typestore)')

    lines.append('  return pos')
    return compile_lines(lines).serialize_ros1  # type: ignore


def generate_deserialize_ros1(fields: list[Field], typename: str) -> CDRDeser:
    """Generate ros1 deserialization function.

    Args:
        fields: Fields of message.
        typename: Message type name.

    Returns:
        Deserializer function.

    """
    # pylint: disable=too-many-branches,too-many-statements
    lines = [
        'import sys',
        'import numpy',
        'from rosbags.serde.messages import SerdeError, get_msgdef',
        'from rosbags.serde.primitives import unpack_bool_le',
        'from rosbags.serde.primitives import unpack_int8_le',
        'from rosbags.serde.primitives import unpack_int16_le',
        'from rosbags.serde.primitives import unpack_int32_le',
        'from rosbags.serde.primitives import unpack_int64_le',
        'from rosbags.serde.primitives import unpack_uint8_le',
        'from rosbags.serde.primitives import unpack_uint16_le',
        'from rosbags.serde.primitives import unpack_uint32_le',
        'from rosbags.serde.primitives import unpack_uint64_le',
        'from rosbags.serde.primitives import unpack_float32_le',
        'from rosbags.serde.primitives import unpack_float64_le',
        'def deserialize_ros1(rawdata, pos, cls, typestore):',
    ]

    if typename == 'std_msgs/msg/Header':
        lines.append('  pos += 4')

    be_syms = ('>',) if sys.byteorder == 'little' else ('=', '>')

    funcname = 'deserialize_ros1'
    lines.append('  values = []')
    for fcurr in fields:
        desc = fcurr[1]

        if desc.valtype == Valtype.MESSAGE:
            lines.append(f'  msgdef = get_msgdef("{desc.args.name}", typestore)')
            lines.append(f'  obj, pos = msgdef.{funcname}(rawdata, pos, msgdef.cls, typestore)')
            lines.append('  values.append(obj)')

        elif desc.valtype == Valtype.BASE:
            if desc.args == 'string':
                lines.append('  length = unpack_int32_le(rawdata, pos)[0]')
                lines.append('  string = bytes(rawdata[pos + 4:pos + 4 + length]).decode()')
                lines.append('  values.append(string)')
                lines.append('  pos += 4 + length')
            else:
                lines.append(f'  value = unpack_{desc.args}_le(rawdata, pos)[0]')
                lines.append('  values.append(value)')
                lines.append(f'  pos += {SIZEMAP[desc.args]}')

        elif desc.valtype == Valtype.ARRAY:
            subdesc, length = desc.args
            if subdesc.valtype == Valtype.BASE:
                if subdesc.args == 'string':
                    lines.append('  value = []')
                    for _ in range(length):
                        lines.append('  length = unpack_int32_le(rawdata, pos)[0]')
                        lines.append(
                            '  value.append(bytes(rawdata[pos + 4:pos + 4 + length]).decode())',
                        )
                        lines.append('  pos += 4 + length')
                    lines.append('  values.append(value)')
                else:
                    size = length * SIZEMAP[subdesc.args]
                    lines.append(
                        f'  val = numpy.frombuffer(rawdata, '
                        f'dtype=numpy.{subdesc.args}, count={length}, offset=pos)',
                    )
                    lines.append(f'  if val.dtype.byteorder in {be_syms}:')
                    lines.append('    val = val.byteswap()')
                    lines.append('  values.append(val)')
                    lines.append(f'  pos += {size}')
            else:
                assert subdesc.valtype == Valtype.MESSAGE
                lines.append(f'  msgdef = get_msgdef("{subdesc.args.name}", typestore)')
                lines.append('  value = []')
                for _ in range(length):
                    lines.append(
                        f'  obj, pos = msgdef.{funcname}(rawdata, pos, msgdef.cls, typestore)',
                    )
                    lines.append('  value.append(obj)')
                lines.append('  values.append(value)')

        else:
            assert desc.valtype == Valtype.SEQUENCE
            lines.append('  size = unpack_int32_le(rawdata, pos)[0]')
            lines.append('  pos += 4')
            subdesc = desc.args[0]

            if subdesc.valtype == Valtype.BASE:
                if subdesc.args == 'string':
                    lines.append('  value = []')
                    lines.append('  for _ in range(size):')
                    lines.append('    length = unpack_int32_le(rawdata, pos)[0]')
                    lines.append(
                        '    value.append(bytes(rawdata[pos + 4:pos + 4 + length])'
                        '.decode())',
                    )
                    lines.append('    pos += 4 + length')
                    lines.append('  values.append(value)')
                else:
                    lines.append(f'  length = size * {SIZEMAP[subdesc.args]}')
                    lines.append(
                        f'  val = numpy.frombuffer(rawdata, '
                        f'dtype=numpy.{subdesc.args}, count=size, offset=pos)',
                    )
                    lines.append(f'  if val.dtype.byteorder in {be_syms}:')
                    lines.append('    val = val.byteswap()')
                    lines.append('  values.append(val)')
                    lines.append('  pos += length')

            if subdesc.valtype == Valtype.MESSAGE:
                lines.append(f'  msgdef = get_msgdef("{subdesc.args.name}", typestore)')
                lines.append('  value = []')
                lines.append('  for _ in range(size):')
                lines.append(
                    f'    obj, pos = msgdef.{funcname}(rawdata, pos, msgdef.cls, typestore)',
                )
                lines.append('    value.append(obj)')
                lines.append('  values.append(value)')

    lines.append('  return cls(*values), pos')
    return compile_lines(lines).deserialize_ros1  # type: ignore
