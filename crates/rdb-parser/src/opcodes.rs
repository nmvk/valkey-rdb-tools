// RDB file opcodes and type constants — mirrors valkey/src/rdb.h

// --- Opcodes ---

pub const RDB_OPCODE_SLOT_IMPORT: u8 = 243;
pub const RDB_OPCODE_SLOT_INFO: u8 = 244;
pub const RDB_OPCODE_FUNCTION2: u8 = 245;
pub const RDB_OPCODE_FUNCTION_PRE_GA: u8 = 246;
pub const RDB_OPCODE_MODULE_AUX: u8 = 247;
pub const RDB_OPCODE_IDLE: u8 = 248;
pub const RDB_OPCODE_FREQ: u8 = 249;
pub const RDB_OPCODE_AUX: u8 = 250;
pub const RDB_OPCODE_RESIZEDB: u8 = 251;
pub const RDB_OPCODE_EXPIRETIME_MS: u8 = 252;
pub const RDB_OPCODE_EXPIRETIME: u8 = 253;
pub const RDB_OPCODE_SELECTDB: u8 = 254;
pub const RDB_OPCODE_EOF: u8 = 255;

// --- Value type codes ---

pub const RDB_TYPE_STRING: u8 = 0;
pub const RDB_TYPE_LIST: u8 = 1;
pub const RDB_TYPE_SET: u8 = 2;
pub const RDB_TYPE_ZSET: u8 = 3;
pub const RDB_TYPE_HASH: u8 = 4;
pub const RDB_TYPE_ZSET_2: u8 = 5;
pub const RDB_TYPE_MODULE_PRE_GA: u8 = 6;
pub const RDB_TYPE_MODULE_2: u8 = 7;
// 8 is unused
pub const RDB_TYPE_HASH_ZIPMAP: u8 = 9;
pub const RDB_TYPE_LIST_ZIPLIST: u8 = 10;
pub const RDB_TYPE_SET_INTSET: u8 = 11;
pub const RDB_TYPE_ZSET_ZIPLIST: u8 = 12;
pub const RDB_TYPE_HASH_ZIPLIST: u8 = 13;
pub const RDB_TYPE_LIST_QUICKLIST: u8 = 14;
pub const RDB_TYPE_STREAM_LISTPACKS: u8 = 15;
pub const RDB_TYPE_HASH_LISTPACK: u8 = 16;
pub const RDB_TYPE_ZSET_LISTPACK: u8 = 17;
pub const RDB_TYPE_LIST_QUICKLIST_2: u8 = 18;
pub const RDB_TYPE_STREAM_LISTPACKS_2: u8 = 19;
pub const RDB_TYPE_SET_LISTPACK: u8 = 20;
pub const RDB_TYPE_STREAM_LISTPACKS_3: u8 = 21;
pub const RDB_TYPE_HASH_2: u8 = 22; // Hash with per-field TTL, RDB 80 (Valkey 9.0)

// --- Length encoding constants ---

pub const RDB_6BITLEN: u8 = 0;
pub const RDB_14BITLEN: u8 = 1;
pub const RDB_ENCVAL: u8 = 3;
pub const RDB_32BITLEN: u8 = 0x80;
pub const RDB_64BITLEN: u8 = 0x81;

// --- String encoding sub-types (when length prefix = 11) ---

pub const RDB_ENC_INT8: u8 = 0;
pub const RDB_ENC_INT16: u8 = 1;
pub const RDB_ENC_INT32: u8 = 2;
pub const RDB_ENC_LZF: u8 = 3;

/// Returns true if the type byte is a valid RDB object type.
pub fn is_object_type(t: u8) -> bool {
    (t <= 7 && t != 8) || (t >= 9 && t <= RDB_TYPE_HASH_2)
}

/// Returns the logical type name for a given RDB type code.
pub fn type_name(t: u8) -> &'static str {
    match t {
        RDB_TYPE_STRING => "string",
        RDB_TYPE_LIST | RDB_TYPE_LIST_ZIPLIST | RDB_TYPE_LIST_QUICKLIST
        | RDB_TYPE_LIST_QUICKLIST_2 => "list",
        RDB_TYPE_SET | RDB_TYPE_SET_INTSET | RDB_TYPE_SET_LISTPACK => "set",
        RDB_TYPE_ZSET | RDB_TYPE_ZSET_2 | RDB_TYPE_ZSET_ZIPLIST
        | RDB_TYPE_ZSET_LISTPACK => "zset",
        RDB_TYPE_HASH | RDB_TYPE_HASH_ZIPMAP | RDB_TYPE_HASH_ZIPLIST
        | RDB_TYPE_HASH_LISTPACK | RDB_TYPE_HASH_2 => "hash",
        RDB_TYPE_STREAM_LISTPACKS | RDB_TYPE_STREAM_LISTPACKS_2
        | RDB_TYPE_STREAM_LISTPACKS_3 => "stream",
        RDB_TYPE_MODULE_PRE_GA | RDB_TYPE_MODULE_2 => "module",
        _ => "unknown",
    }
}

/// Returns the encoding name for a given RDB type code.
pub fn encoding_name(t: u8) -> &'static str {
    match t {
        RDB_TYPE_STRING => "string",
        RDB_TYPE_LIST => "linkedlist",
        RDB_TYPE_SET => "hashtable",
        RDB_TYPE_ZSET => "skiplist",
        RDB_TYPE_ZSET_2 => "skiplist",
        RDB_TYPE_HASH => "hashtable",
        RDB_TYPE_HASH_ZIPMAP => "zipmap",
        RDB_TYPE_LIST_ZIPLIST => "ziplist",
        RDB_TYPE_SET_INTSET => "intset",
        RDB_TYPE_ZSET_ZIPLIST => "ziplist",
        RDB_TYPE_HASH_ZIPLIST => "ziplist",
        RDB_TYPE_LIST_QUICKLIST => "quicklist",
        RDB_TYPE_LIST_QUICKLIST_2 => "quicklist2",
        RDB_TYPE_HASH_LISTPACK => "listpack",
        RDB_TYPE_ZSET_LISTPACK => "listpack",
        RDB_TYPE_SET_LISTPACK => "listpack",
        RDB_TYPE_STREAM_LISTPACKS => "stream",
        RDB_TYPE_STREAM_LISTPACKS_2 => "stream2",
        RDB_TYPE_STREAM_LISTPACKS_3 => "stream3",
        RDB_TYPE_HASH_2 => "hashtable",
        RDB_TYPE_MODULE_PRE_GA | RDB_TYPE_MODULE_2 => "module",
        _ => "unknown",
    }
}
