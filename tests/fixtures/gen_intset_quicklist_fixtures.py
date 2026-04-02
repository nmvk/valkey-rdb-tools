#!/usr/bin/env python3
"""Generate RDB fixtures for intset and quicklist encoded types.

Generates:
  - set_intset.rdb     — SET_INTSET (type 11): small integer set
  - list_quicklist.rdb  — LIST_QUICKLIST v1 (type 14): list of ziplist nodes
  - list_quicklist2.rdb — LIST_QUICKLIST_2 (type 18): list of listpack nodes
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


# --- RDB helpers ---

def rdb_len_encode(n):
    if n < 64:
        return struct.pack('B', n)
    elif n < 16384:
        return struct.pack('>H', 0x4000 | n)
    elif n < (1 << 32):
        return struct.pack('B', 0x80) + struct.pack('>I', n)
    else:
        return struct.pack('B', 0x81) + struct.pack('>Q', n)

def rdb_string(data):
    return rdb_len_encode(len(data)) + data

def make_rdb(kv_entries):
    """Build a complete RDB file (REDIS version 11).
    kv_entries: list of (type_code, key_bytes, value_blob_bytes)
    For intset/quicklist the value_blob_bytes is the raw blob for intset,
    or a pre-built sequence of length+nodes for quicklist.
    """
    parts = []
    parts.append(b'REDIS0011')
    parts.append(struct.pack('BB', 0xFE, 0x00))  # SELECTDB 0
    parts.append(struct.pack('B', 0xFB))  # RESIZEDB
    parts.append(rdb_len_encode(len(kv_entries)))
    parts.append(rdb_len_encode(0))

    for entry in kv_entries:
        parts.append(entry)

    parts.append(struct.pack('B', 0xFF))  # EOF
    data = b''.join(parts)
    checksum = crc64(data)
    data += struct.pack('<Q', checksum)
    return data


# --- Intset ---

def make_intset(encoding, values):
    """Build an intset blob. encoding: 2=i16, 4=i32, 8=i64."""
    fmt = {2: '<h', 4: '<i', 8: '<q'}[encoding]
    buf = struct.pack('<II', encoding, len(values))
    for v in values:
        buf += struct.pack(fmt, v)
    return buf

def gen_set_intset():
    """SET_INTSET (type 11): int16-encoded set with 5 members."""
    blob = make_intset(2, [-100, -1, 0, 42, 1000])
    parts = []
    parts.append(struct.pack('B', 11))  # RDB_TYPE_SET_INTSET
    parts.append(rdb_string(b'myset'))
    parts.append(rdb_string(blob))
    return make_rdb([b''.join(parts)])


# --- Ziplist helpers (for quicklist v1 nodes) ---

def zl_entry_str(prevlen, s):
    parts = []
    if prevlen < 254:
        parts.append(struct.pack('B', prevlen))
    else:
        parts.append(struct.pack('<BI', 0xFE, prevlen))
    parts.append(struct.pack('B', len(s)))  # 6-bit string
    parts.append(s)
    return b''.join(parts)

def zl_entry_imm(prevlen, val):
    """4-bit immediate integer (0-12)."""
    assert 0 <= val <= 12
    parts = []
    if prevlen < 254:
        parts.append(struct.pack('B', prevlen))
    else:
        parts.append(struct.pack('<BI', 0xFE, prevlen))
    parts.append(struct.pack('B', 0xF1 + val))
    return b''.join(parts)

def make_ziplist(entries):
    body = b''.join(entries)
    total = 10 + len(body) + 1
    if entries:
        offset = 10
        for e in entries[:-1]:
            offset += len(e)
        zltail = offset
    else:
        zltail = 10
    header = struct.pack('<IIH', total, zltail, len(entries))
    return header + body + b'\xFF'


# --- Listpack helpers (for quicklist v2 nodes) ---

def lp_backlen(entry_len):
    """Encode the backlen for a listpack entry."""
    if entry_len <= 127:
        return struct.pack('B', entry_len)
    # Multi-byte backlen
    parts = []
    remaining = entry_len
    while remaining > 0:
        byte = remaining & 0x7F
        remaining >>= 7
        if remaining > 0:
            byte |= 0x80
        parts.append(struct.pack('B', byte))
    return b''.join(parts)

def lp_entry_7bit(val):
    """Encode a 7-bit unsigned integer listpack entry."""
    assert 0 <= val <= 127
    enc = struct.pack('B', val)
    bl = lp_backlen(len(enc))
    return enc + bl

def lp_entry_str6(s):
    """Encode a 6-bit string listpack entry."""
    assert len(s) < 64
    enc = struct.pack('B', 0x80 | len(s)) + s
    bl = lp_backlen(len(enc))
    return enc + bl

def lp_entry_i16(val):
    """Encode a 16-bit signed integer listpack entry."""
    enc = struct.pack('B', 0xF1) + struct.pack('<h', val)
    bl = lp_backlen(len(enc))
    return enc + bl

def make_listpack(entries):
    """Build a listpack blob from encoded entries."""
    body = b''.join(entries)
    total = 7 + len(body)  # 4-byte total + 2-byte num_elem + entries + 0xFF
    header = struct.pack('<IH', total, len(entries))
    return header + body + b'\xFF'


# --- Quicklist v1 (type 14) ---

def gen_list_quicklist():
    """LIST_QUICKLIST (type 14): 2 ziplist nodes."""
    # Node 1: ["hello", "world"]
    e1 = zl_entry_str(0, b'hello')
    e2 = zl_entry_str(len(e1), b'world')
    zl1 = make_ziplist([e1, e2])

    # Node 2: ["foo", 7]
    e3 = zl_entry_str(0, b'foo')
    e4 = zl_entry_imm(len(e3), 7)
    zl2 = make_ziplist([e3, e4])

    parts = []
    parts.append(struct.pack('B', 14))  # RDB_TYPE_LIST_QUICKLIST
    parts.append(rdb_string(b'mylist'))
    parts.append(rdb_len_encode(2))  # 2 nodes
    parts.append(rdb_string(zl1))    # node 1
    parts.append(rdb_string(zl2))    # node 2
    return make_rdb([b''.join(parts)])


# --- Quicklist v2 (type 18) ---

def gen_list_quicklist2():
    """LIST_QUICKLIST_2 (type 18): 2 packed nodes + 1 plain node."""
    # Packed node 1 (container=2): listpack with ["alpha", "beta"]
    lp_e1 = lp_entry_str6(b'alpha')
    lp_e2 = lp_entry_str6(b'beta')
    lp1 = make_listpack([lp_e1, lp_e2])

    # Packed node 2 (container=2): listpack with [42, 100]
    lp_e3 = lp_entry_7bit(42)
    lp_e4 = lp_entry_7bit(100)
    lp2 = make_listpack([lp_e3, lp_e4])

    # Plain node (container=1): single raw element "standalone"
    plain_val = b'standalone'

    parts = []
    parts.append(struct.pack('B', 18))  # RDB_TYPE_LIST_QUICKLIST_2
    parts.append(rdb_string(b'mylist2'))
    parts.append(rdb_len_encode(3))     # 3 nodes
    # Node 1: packed
    parts.append(rdb_len_encode(2))     # container=2 (PACKED)
    parts.append(rdb_string(lp1))
    # Node 2: packed
    parts.append(rdb_len_encode(2))     # container=2 (PACKED)
    parts.append(rdb_string(lp2))
    # Node 3: plain
    parts.append(rdb_len_encode(1))     # container=1 (PLAIN)
    parts.append(rdb_string(plain_val))

    return make_rdb([b''.join(parts)])


# --- Main ---

if __name__ == '__main__':
    script_dir = os.path.dirname(os.path.abspath(__file__))

    fixtures = {
        'set_intset.rdb': gen_set_intset(),
        'list_quicklist.rdb': gen_list_quicklist(),
        'list_quicklist2.rdb': gen_list_quicklist2(),
    }

    for name, data in fixtures.items():
        path = os.path.join(script_dir, name)
        with open(path, 'wb') as f:
            f.write(data)
        print(f"  wrote {path} ({len(data)} bytes)")
