#!/usr/bin/env python3
"""Generate RDB fixtures with ziplist-encoded types.

Modern Valkey uses listpack encoding internally, so we can't generate ziplist-
encoded RDB files from the server. This script writes minimal valid RDB files
that use the legacy ziplist type codes (10, 12, 13).

Each fixture is a complete RDB file:
  REDIS0011 header + SELECTDB 0 + RESIZEDB + key-value(s) + EOF + CRC64
"""

import struct
import sys
import os

# CRC-64 matching Valkey (reflected poly 0x95ac9329ac4bc9b5)
def _make_crc_table():
    poly = 0x95AC9329AC4BC9B5
    table = []
    for i in range(256):
        crc = 0
        for j in range(8):
            if ((i >> j) ^ crc) & 1:
                crc = (crc >> 1) ^ poly
            else:
                crc >>= 1
        table.append(crc)
    return table

_CRC_TABLE = _make_crc_table()

def crc64(data, crc=0):
    for b in data:
        idx = (crc ^ b) & 0xFF
        crc = _CRC_TABLE[idx] ^ (crc >> 8)
    return crc


# --- Ziplist encoding helpers ---

def zl_entry_str(prevlen, s):
    """Encode a ziplist string entry with 6-bit length."""
    assert len(s) < 64
    parts = []
    # prevlen
    if prevlen < 254:
        parts.append(struct.pack('B', prevlen))
    else:
        parts.append(struct.pack('<BI', 0xFE, prevlen))
    # encoding: 00xxxxxx
    parts.append(struct.pack('B', len(s)))
    parts.append(s)
    return b''.join(parts)


def zl_entry_str14(prevlen, s):
    """Encode a ziplist string entry with 14-bit length (big endian)."""
    assert len(s) < 16384
    parts = []
    if prevlen < 254:
        parts.append(struct.pack('B', prevlen))
    else:
        parts.append(struct.pack('<BI', 0xFE, prevlen))
    # encoding: 01xxxxxx xxxxxxxx (big endian)
    high = 0x40 | ((len(s) >> 8) & 0x3F)
    low = len(s) & 0xFF
    parts.append(struct.pack('BB', high, low))
    parts.append(s)
    return b''.join(parts)


def zl_entry_imm(prevlen, val):
    """Encode a 4-bit immediate integer (0-12)."""
    assert 0 <= val <= 12
    parts = []
    if prevlen < 254:
        parts.append(struct.pack('B', prevlen))
    else:
        parts.append(struct.pack('<BI', 0xFE, prevlen))
    parts.append(struct.pack('B', 0xF1 + val))
    return b''.join(parts)


def zl_entry_i16(prevlen, val):
    """Encode a 16-bit signed integer."""
    parts = []
    if prevlen < 254:
        parts.append(struct.pack('B', prevlen))
    else:
        parts.append(struct.pack('<BI', 0xFE, prevlen))
    parts.append(struct.pack('B', 0xC0))
    parts.append(struct.pack('<h', val))
    return b''.join(parts)


def zl_entry_i8(prevlen, val):
    """Encode an 8-bit signed integer."""
    parts = []
    if prevlen < 254:
        parts.append(struct.pack('B', prevlen))
    else:
        parts.append(struct.pack('<BI', 0xFE, prevlen))
    parts.append(struct.pack('B', 0xFE))
    parts.append(struct.pack('b', val))
    return b''.join(parts)


def make_ziplist(entries):
    """Build a complete ziplist blob from a list of encoded entry byte strings."""
    body = b''.join(entries)
    total = 10 + len(body) + 1  # header + entries + end byte
    # zltail: offset of the last entry (we compute it properly)
    if entries:
        offset = 10
        for e in entries[:-1]:
            offset += len(e)
        zltail = offset
    else:
        zltail = 10
    header = struct.pack('<IIH', total, zltail, len(entries))
    return header + body + b'\xFF'


def rdb_len_encode(n):
    """Encode a length using RDB length encoding."""
    if n < 64:
        return struct.pack('B', n)
    elif n < 16384:
        return struct.pack('>H', 0x4000 | n)
    elif n < (1 << 32):
        return struct.pack('B', 0x80) + struct.pack('>I', n)
    else:
        return struct.pack('B', 0x81) + struct.pack('>Q', n)


def rdb_string(data):
    """Encode a string with RDB length prefix."""
    return rdb_len_encode(len(data)) + data


def make_rdb(kv_entries):
    """Build a complete RDB file (REDIS version 11 format).

    kv_entries: list of (type_code, key_bytes, value_blob_bytes)
    """
    parts = []
    # Header
    parts.append(b'REDIS0011')
    # SELECTDB 0
    parts.append(struct.pack('BB', 0xFE, 0x00))
    # RESIZEDB
    parts.append(struct.pack('B', 0xFB))
    parts.append(rdb_len_encode(len(kv_entries)))
    parts.append(rdb_len_encode(0))

    for type_code, key, value_blob in kv_entries:
        parts.append(struct.pack('B', type_code))
        parts.append(rdb_string(key))
        parts.append(rdb_string(value_blob))

    # EOF
    parts.append(struct.pack('B', 0xFF))

    data = b''.join(parts)
    checksum = crc64(data)
    data += struct.pack('<Q', checksum)
    return data


# --- Fixture generation ---

def gen_list_ziplist():
    """LIST_ZIPLIST (type 10): list with string and integer elements."""
    e1 = zl_entry_str(0, b'alpha')
    e2 = zl_entry_str(len(e1), b'beta')
    e3 = zl_entry_imm(len(e2), 7)
    e4 = zl_entry_i16(len(e3), -500)
    zl = make_ziplist([e1, e2, e3, e4])
    return make_rdb([(10, b'mylist', zl)])


def gen_hash_ziplist():
    """HASH_ZIPLIST (type 13): hash with field-value pairs."""
    e1 = zl_entry_str(0, b'name')
    e2 = zl_entry_str(len(e1), b'alice')
    e3 = zl_entry_str(len(e2), b'age')
    e4 = zl_entry_imm(len(e3), 10)  # age=10
    e5 = zl_entry_str(len(e4), b'city')
    e6 = zl_entry_str(len(e5), b'nyc')
    zl = make_ziplist([e1, e2, e3, e4, e5, e6])
    return make_rdb([(13, b'myhash', zl)])


def gen_zset_ziplist():
    """ZSET_ZIPLIST (type 12): sorted set with member-score pairs."""
    e1 = zl_entry_str(0, b'first')
    e2 = zl_entry_str(len(e1), b'1.5')  # score as string
    e3 = zl_entry_str(len(e2), b'second')
    e4 = zl_entry_str(len(e3), b'2.7')
    e5 = zl_entry_str(len(e4), b'third')
    e6 = zl_entry_imm(len(e5), 3)  # score=3 as integer
    zl = make_ziplist([e1, e2, e3, e4, e5, e6])
    return make_rdb([(12, b'myzset', zl)])


def gen_ziplist_all():
    """All three ziplist types in one RDB file."""
    # List
    le1 = zl_entry_str(0, b'one')
    le2 = zl_entry_str(len(le1), b'two')
    le3 = zl_entry_imm(len(le2), 3)
    list_zl = make_ziplist([le1, le2, le3])

    # Hash
    he1 = zl_entry_str(0, b'key')
    he2 = zl_entry_str(len(he1), b'val')
    hash_zl = make_ziplist([he1, he2])

    # Sorted set
    ze1 = zl_entry_str(0, b'member')
    ze2 = zl_entry_str(len(ze1), b'1.0')
    zset_zl = make_ziplist([ze1, ze2])

    return make_rdb([
        (10, b'mylist', list_zl),
        (13, b'myhash', hash_zl),
        (12, b'myzset', zset_zl),
    ])


if __name__ == '__main__':
    outdir = os.path.dirname(os.path.abspath(__file__))

    fixtures = {
        'list_ziplist.rdb': gen_list_ziplist(),
        'hash_ziplist.rdb': gen_hash_ziplist(),
        'zset_ziplist.rdb': gen_zset_ziplist(),
        'ziplist_all.rdb': gen_ziplist_all(),
    }

    for name, data in fixtures.items():
        path = os.path.join(outdir, name)
        with open(path, 'wb') as f:
            f.write(data)
        print(f'Wrote {path} ({len(data)} bytes)')
