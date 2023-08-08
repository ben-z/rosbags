# Copyright 2020-2023  Ternaris.
# SPDX-License-Identifier: Apache-2.0
"""Types and helpers used by message definition converters."""

from __future__ import annotations

import json
import keyword
from enum import IntEnum, auto
from hashlib import sha256
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Dict, List, Literal, Tuple, Union

    from .peg import Visitor
    from .register import Typestore

    Basetype = Union[str, Tuple[Literal['string'], int]]
    Constdefs = List[Tuple[str, str, Any]]
    Fielddesc = Union[Tuple[Literal[1], Basetype], Tuple[Literal[2], str],
                      Tuple[Literal[3, 4], Tuple[Union[Tuple[Literal[1], Basetype],
                                                       Tuple[Literal[2], str]], int]]]
    Fielddefs = List[Tuple[str, Fielddesc]]
    Typesdict = Dict[str, Tuple[Constdefs, Fielddefs]]


class TypesysError(Exception):
    """Parser error."""


class Nodetype(IntEnum):
    """Parse tree node types.

    The first four match the Valtypes of final message definitions.
    """

    BASE = auto()
    NAME = auto()
    ARRAY = auto()
    SEQUENCE = auto()

    LITERAL_STRING = auto()
    LITERAL_NUMBER = auto()
    LITERAL_BOOLEAN = auto()
    LITERAL_CHAR = auto()

    MODULE = auto()
    CONST = auto()
    STRUCT = auto()
    SDECLARATOR = auto()
    ADECLARATOR = auto()
    ANNOTATION = auto()
    EXPRESSION_BINARY = auto()
    EXPRESSION_UNARY = auto()


def normalize_fieldname(name: str) -> str:
    """Normalize field name.

    Avoid collisions with Python keywords.

    Args:
        name: Field name.

    Returns:
        Normalized name.

    """
    if keyword.iskeyword(name):
        return f'{name}_'
    return name


def parse_message_definition(visitor: Visitor, text: str) -> Typesdict:
    """Parse message definition.

    Args:
        visitor: Visitor instance to use.
        text: Message definition.

    Returns:
        Parsetree of message.

    Raises:
        TypesysError: Message parsing failed.

    """
    try:
        rule = visitor.RULES['specification']
        pos = rule.skip_ws(text, 0)
        npos, trees = rule.parse(text, pos)
        assert npos == len(text), f'Could not parse: {text!r}'
        return visitor.visit(trees)  # type: ignore
    except Exception as err:
        raise TypesysError(f'Could not parse: {text!r}') from err


TIDMAP = {
    'int8': 2,
    'uint8': 3,
    'int16': 4,
    'uint16': 5,
    'int32': 6,
    'uint32': 7,
    'int64': 8,
    'uint64': 9,
    'float32': 10,
    'float64': 11,
    'float128': 12,
    'char': 13,
    # 'wchar': 14,
    'bool': 15,
    'octet': 16,
    'string': 17,
    # 'wstring': 18,
    # 'fixed_string': 19,
    # 'fixed_wstring': 20,
    'bounded_string': 21,
    # 'bounded_wstring': 22,
}


def hash_rihs01(typ: str, typestore: Typestore) -> str:
    """Hash message definition.

    Args:
        typ: Message type name.
        typestore: Message type store.

    Returns:
        Hash value.

    """

    def get_field(name: str, desc: Fielddesc) -> dict[str, Any]:
        increment = 0
        capacity = 0
        string_capacity = 0
        subtype = ''
        if desc[0] == 3:
            increment = 48
            capacity = desc[1][1]
            typ, rest = desc[1][0]
        elif desc[0] == 4:
            count = desc[1][1]
            if count:
                increment = 96
                capacity = count
            else:
                increment = 144
            typ, rest = desc[1][0]
        else:
            typ, rest = desc

        # assert isinstance(rest, (str, tuple))
        if typ == 2:
            tid = increment + 1
            assert isinstance(rest, str)
            subtype = rest
            get_struct(subtype)
        else:
            if isinstance(rest, tuple):
                assert isinstance(rest[0], str)
                if rest[1]:
                    string_capacity = rest[1]
                    tid = increment + TIDMAP['bounded_string']
                else:
                    tid = increment + TIDMAP['string']
            else:
                tid = increment + TIDMAP[rest]

        return {
            'name': name,
            'type': {
                'type_id': tid,
                'capacity': capacity,
                'string_capacity': string_capacity,
                'nested_type_name': subtype,
            },
        }

    struct_cache = {}

    def get_struct(typ: str) -> dict[str, Any]:
        if typ not in struct_cache:
            struct_cache[typ] = {
                'type_name': typ,
                'fields': [
                    get_field(x, y) for x, y in typestore.FIELDDEFS[typ][1] or
                    [('structure_needs_at_least_one_member', (1, 'uint8'))]
                ],
            }
        return struct_cache[typ]

    dct = {
        'type_description': get_struct(typ),
        'referenced_type_descriptions': [y for x, y in sorted(struct_cache.items()) if x != typ],
    }

    digest = sha256(json.dumps(dct).encode()).hexdigest()
    return f'RIHS01_{digest}'
