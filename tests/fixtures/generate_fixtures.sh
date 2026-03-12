#!/usr/bin/env bash
#
# Generate RDB test fixtures using a local Valkey server.
#
# Usage:
#   ./generate_fixtures.sh [port]
#
# Prerequisites:
#   - Valkey built at /Users/raghav/valkeys/valkey/src/
#   - Or valkey-server/valkey-cli in PATH
#
# The script starts a temporary server, inserts known data, saves RDB files,
# and shuts down. All fixtures are written to the same directory as this script.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PORT="${1:-6399}"
DIR=$(mktemp -d)
PIDFILE="$DIR/valkey.pid"

# Prefer local Valkey build, then PATH
VALKEY_SRC="/Users/raghav/valkeys/valkey/src"
if [ -x "$VALKEY_SRC/valkey-server" ]; then
    SERVER="$VALKEY_SRC/valkey-server"
    CLI="$VALKEY_SRC/valkey-cli"
elif command -v valkey-server &>/dev/null; then
    SERVER="valkey-server"
    CLI="valkey-cli"
else
    echo "Error: valkey-server not found" >&2
    echo "Build Valkey at /Users/raghav/valkeys/valkey/ or add to PATH" >&2
    exit 1
fi

cli() {
    "$CLI" -p "$PORT" "$@"
}

wait_for_bgsave() {
    sleep 0.5
    while [ "$(cli INFO persistence 2>/dev/null | grep rdb_bgsave_in_progress | tr -d '\r' | cut -d: -f2)" != "0" ]; do
        sleep 0.2
    done
}

cleanup() {
    echo "Shutting down server..."
    cli SHUTDOWN NOSAVE 2>/dev/null || true
    rm -rf "$DIR"
}
trap cleanup EXIT

echo "Using: $SERVER"
echo "$($SERVER --version)"
echo "Starting on port $PORT (dir: $DIR)..."

"$SERVER" \
    --port "$PORT" \
    --dir "$DIR" \
    --dbfilename dump.rdb \
    --save "" \
    --appendonly no \
    --daemonize yes \
    --pidfile "$PIDFILE" \
    --loglevel warning

# Wait for server to be ready
for i in $(seq 1 30); do
    if cli PING 2>/dev/null | grep -q PONG; then
        break
    fi
    sleep 0.1
done

echo "Server ready."
echo ""

# -----------------------------------------------------------
# basic.rdb — one key of each type, small values
# -----------------------------------------------------------
echo "Generating basic.rdb..."
cli FLUSHALL >/dev/null

cli SET mystring "hello world" >/dev/null
cli LPUSH mylist c b a >/dev/null
cli SADD myset x y z >/dev/null
cli ZADD myzset 1.5 alice 2.7 bob 0.3 charlie >/dev/null
cli HSET myhash field1 value1 field2 value2 field3 value3 >/dev/null

# Add a key with TTL
cli SET expiring_key "gone soon" >/dev/null
cli PEXPIREAT expiring_key 4102444800000 >/dev/null  # 2100-01-01

# Verify encodings
echo "  Encodings:"
echo "    mystring:  $(cli OBJECT ENCODING mystring)"
echo "    mylist:    $(cli OBJECT ENCODING mylist)"
echo "    myset:     $(cli OBJECT ENCODING myset)"
echo "    myzset:    $(cli OBJECT ENCODING myzset)"
echo "    myhash:    $(cli OBJECT ENCODING myhash)"

cli BGSAVE >/dev/null
wait_for_bgsave
cp "$DIR/dump.rdb" "$SCRIPT_DIR/basic.rdb"
echo "  -> basic.rdb saved ($(wc -c < "$SCRIPT_DIR/basic.rdb") bytes)"

# -----------------------------------------------------------
# empty.rdb — no keys
# -----------------------------------------------------------
echo "Generating empty.rdb..."
cli FLUSHALL >/dev/null
cli BGSAVE >/dev/null
wait_for_bgsave
cp "$DIR/dump.rdb" "$SCRIPT_DIR/empty.rdb"
echo "  -> empty.rdb saved ($(wc -c < "$SCRIPT_DIR/empty.rdb") bytes)"

# -----------------------------------------------------------
# multi_db.rdb — data in db 0 and db 1
# -----------------------------------------------------------
echo "Generating multi_db.rdb..."
cli FLUSHALL >/dev/null

# db 0
cli SELECT 0 >/dev/null
cli SET db0_key "in database zero" >/dev/null
cli HSET db0_hash name "db0" >/dev/null

# db 1
cli SELECT 1 >/dev/null
cli SET db1_key "in database one" >/dev/null
cli LPUSH db1_list alpha beta gamma >/dev/null

cli BGSAVE >/dev/null
wait_for_bgsave
cp "$DIR/dump.rdb" "$SCRIPT_DIR/multi_db.rdb"
echo "  -> multi_db.rdb saved ($(wc -c < "$SCRIPT_DIR/multi_db.rdb") bytes)"

# -----------------------------------------------------------
# encodings.rdb — force different encodings via thresholds
# -----------------------------------------------------------
echo "Generating encodings.rdb..."
cli FLUSHALL >/dev/null
cli SELECT 0 >/dev/null

# Small hash → listpack encoding
cli HSET small_hash f1 v1 f2 v2 >/dev/null

# Large hash → hashtable encoding (exceed hash-max-listpack-entries default 128)
for i in $(seq 1 200); do
    cli HSET big_hash "field_$i" "value_$i" >/dev/null
done

# Integer set → intset encoding
cli SADD int_set 1 2 3 100 999 42 >/dev/null

# Small set → listpack
cli SADD small_set "hello" "world" "foo" >/dev/null

# Large set → hashtable (exceed set-max-listpack-entries default 128)
for i in $(seq 1 200); do
    cli SADD big_set "member_$i" >/dev/null
done

# Small sorted set → listpack
cli ZADD small_zset 1.0 a 2.0 b 3.0 c >/dev/null

# Large sorted set → skiplist
for i in $(seq 1 200); do
    cli ZADD big_zset "$i.5" "member_$i" >/dev/null
done

# Small list → listpack
cli RPUSH small_list a b c d e >/dev/null

# Large list → quicklist
for i in $(seq 1 500); do
    cli RPUSH big_list "element_$i" >/dev/null
done

# LZF compressed string (> 20 bytes of compressible data)
cli SET lzf_string "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" >/dev/null

# Integer-encoded strings
cli SET int_string_small "42" >/dev/null
cli SET int_string_large "1234567890" >/dev/null

echo "  Encodings:"
echo "    small_hash:  $(cli OBJECT ENCODING small_hash)"
echo "    big_hash:    $(cli OBJECT ENCODING big_hash)"
echo "    int_set:     $(cli OBJECT ENCODING int_set)"
echo "    small_set:   $(cli OBJECT ENCODING small_set)"
echo "    big_set:     $(cli OBJECT ENCODING big_set)"
echo "    small_zset:  $(cli OBJECT ENCODING small_zset)"
echo "    big_zset:    $(cli OBJECT ENCODING big_zset)"
echo "    small_list:  $(cli OBJECT ENCODING small_list)"
echo "    big_list:    $(cli OBJECT ENCODING big_list)"
echo "    lzf_string:  $(cli OBJECT ENCODING lzf_string)"
echo "    int_string:  $(cli OBJECT ENCODING int_string_small)"

cli BGSAVE >/dev/null
wait_for_bgsave
cp "$DIR/dump.rdb" "$SCRIPT_DIR/encodings.rdb"
echo "  -> encodings.rdb saved ($(wc -c < "$SCRIPT_DIR/encodings.rdb") bytes)"

# -----------------------------------------------------------
# expiry.rdb — keys with various TTL states
# -----------------------------------------------------------
echo "Generating expiry.rdb..."
cli FLUSHALL >/dev/null
cli SELECT 0 >/dev/null

# Key with no TTL
cli SET no_ttl_key "persistent" >/dev/null

# Key with far-future TTL
cli SET future_ttl_key "expires later" >/dev/null
cli PEXPIREAT future_ttl_key 4102444800000 >/dev/null  # 2100-01-01

# Key with past TTL (already expired but still in RDB)
cli SET past_ttl_key "already expired" >/dev/null
cli PEXPIREAT past_ttl_key 1000000000000 >/dev/null  # 2001-09-09

# Multiple types with TTL
cli LPUSH ttl_list a b c >/dev/null
cli PEXPIREAT ttl_list 4102444800000 >/dev/null

cli HSET ttl_hash f1 v1 >/dev/null
cli PEXPIREAT ttl_hash 4102444800000 >/dev/null

cli BGSAVE >/dev/null
wait_for_bgsave
cp "$DIR/dump.rdb" "$SCRIPT_DIR/expiry.rdb"
echo "  -> expiry.rdb saved ($(wc -c < "$SCRIPT_DIR/expiry.rdb") bytes)"

# -----------------------------------------------------------
# hash_field_ttl.rdb — Valkey 9.0 HASH_2 with per-field expiry
# -----------------------------------------------------------
echo "Generating hash_field_ttl.rdb..."
cli FLUSHALL >/dev/null
cli SELECT 0 >/dev/null

# Hash with per-field TTL (requires Valkey 9.0+ / RDB 80)
cli HSET hfe_hash field_persist "no expiry" >/dev/null
cli HSET hfe_hash field_future "expires later" >/dev/null
cli HSET hfe_hash field_short "expires soon" >/dev/null

# Set per-field expiry using HPEXPIREAT
cli HPEXPIREAT hfe_hash 4102444800000 FIELDS 1 field_future >/dev/null  # 2100-01-01
cli HPEXPIREAT hfe_hash 4102444800000 FIELDS 1 field_short >/dev/null   # 2100-01-01

# Also a normal hash without field TTL for comparison
cli HSET normal_hash f1 v1 f2 v2 >/dev/null

echo "  Encoding:"
echo "    hfe_hash:    $(cli OBJECT ENCODING hfe_hash)"
echo "    normal_hash: $(cli OBJECT ENCODING normal_hash)"

# Verify field TTLs
echo "  Field TTLs (HTTL):"
echo "    hfe_hash:    $(cli HTTL hfe_hash FIELDS 3 field_persist field_future field_short)"

cli BGSAVE >/dev/null
wait_for_bgsave
cp "$DIR/dump.rdb" "$SCRIPT_DIR/hash_field_ttl.rdb"
echo "  -> hash_field_ttl.rdb saved ($(wc -c < "$SCRIPT_DIR/hash_field_ttl.rdb") bytes)"

# -----------------------------------------------------------
# streams.rdb — streams with entries and consumer groups
# -----------------------------------------------------------
echo "Generating streams.rdb..."
cli FLUSHALL >/dev/null
cli SELECT 0 >/dev/null

# Simple stream with entries
cli XADD mystream '*' name alice age 30 >/dev/null
cli XADD mystream '*' name bob age 25 >/dev/null
cli XADD mystream '*' name charlie age 35 >/dev/null

# Stream with consumer group
cli XADD grouped_stream '*' event login user alice >/dev/null
cli XADD grouped_stream '*' event purchase user bob item widget >/dev/null
cli XADD grouped_stream '*' event logout user alice >/dev/null

cli XGROUP CREATE grouped_stream mygroup 0 >/dev/null

# Read some entries to create pending entries (PEL)
cli XREADGROUP GROUP mygroup consumer1 COUNT 2 STREAMS grouped_stream '>' >/dev/null
cli XREADGROUP GROUP mygroup consumer2 COUNT 1 STREAMS grouped_stream '>' >/dev/null

# ACK one entry from consumer1 to have mixed PEL state
FIRST_ID=$(cli XRANGE grouped_stream - + COUNT 1 | head -1)
cli XACK grouped_stream mygroup "$FIRST_ID" >/dev/null 2>/dev/null || true

echo "  Stream info:"
echo "    mystream length:       $(cli XLEN mystream)"
echo "    grouped_stream length: $(cli XLEN grouped_stream)"
echo "    mystream encoding:     $(cli OBJECT ENCODING mystream)"

cli BGSAVE >/dev/null
wait_for_bgsave
cp "$DIR/dump.rdb" "$SCRIPT_DIR/streams.rdb"
echo "  -> streams.rdb saved ($(wc -c < "$SCRIPT_DIR/streams.rdb") bytes)"

# -----------------------------------------------------------
# redis_compat.rdb — generated with Redis OSS for REDIS magic coverage
# -----------------------------------------------------------
if command -v redis-server &>/dev/null; then
    REDIS_VER=$(redis-server --version)
    echo "Generating redis_compat.rdb (using Redis OSS)..."
    echo "  $REDIS_VER"

    # Shut down Valkey, start Redis on same port
    cli SHUTDOWN NOSAVE 2>/dev/null || true
    sleep 0.5

    REDIS_DIR=$(mktemp -d)
    redis-server \
        --port "$PORT" \
        --dir "$REDIS_DIR" \
        --dbfilename dump.rdb \
        --save "" \
        --appendonly no \
        --daemonize yes \
        --pidfile "$REDIS_DIR/redis.pid" \
        --loglevel warning

    # Override cleanup to handle Redis shutdown
    cleanup() {
        echo "Shutting down server..."
        redis-cli -p "$PORT" SHUTDOWN NOSAVE 2>/dev/null || true
        rm -rf "$DIR" "$REDIS_DIR"
    }

    for i in $(seq 1 30); do
        if redis-cli -p "$PORT" PING 2>/dev/null | grep -q PONG; then break; fi
        sleep 0.1
    done

    # Use redis-cli for this fixture
    rcli() { redis-cli -p "$PORT" "$@"; }

    rcli FLUSHALL >/dev/null

    # One of each type — same data as basic.rdb for comparison
    rcli SET mystring "hello world" >/dev/null
    rcli LPUSH mylist c b a >/dev/null
    rcli SADD myset x y z >/dev/null
    rcli ZADD myzset 1.5 alice 2.7 bob 0.3 charlie >/dev/null
    rcli HSET myhash field1 value1 field2 value2 field3 value3 >/dev/null
    rcli SET expiring_key "gone soon" >/dev/null
    rcli PEXPIREAT expiring_key 4102444800000 >/dev/null

    echo "  Encodings:"
    echo "    mystring:  $(rcli OBJECT ENCODING mystring)"
    echo "    mylist:    $(rcli OBJECT ENCODING mylist)"
    echo "    myset:     $(rcli OBJECT ENCODING myset)"
    echo "    myzset:    $(rcli OBJECT ENCODING myzset)"
    echo "    myhash:    $(rcli OBJECT ENCODING myhash)"

    rcli BGSAVE >/dev/null
    sleep 0.5
    while [ "$(rcli INFO persistence 2>/dev/null | grep rdb_bgsave_in_progress | tr -d '\r' | cut -d: -f2)" != "0" ]; do
        sleep 0.2
    done
    cp "$REDIS_DIR/dump.rdb" "$SCRIPT_DIR/redis_compat.rdb"
    echo "  -> redis_compat.rdb saved ($(wc -c < "$SCRIPT_DIR/redis_compat.rdb") bytes)"
    echo "  Magic: $(xxd -l 9 "$SCRIPT_DIR/redis_compat.rdb" | head -1 | awk '{print $6$7}')"
else
    echo "Skipping redis_compat.rdb (redis-server not found in PATH)"
fi

echo ""
echo "All fixtures generated successfully!"
echo ""
ls -lh "$SCRIPT_DIR"/*.rdb
echo ""
echo "Headers:"
for f in "$SCRIPT_DIR"/*.rdb; do
    echo "  $(basename $f): $(head -c 9 "$f")"
done
