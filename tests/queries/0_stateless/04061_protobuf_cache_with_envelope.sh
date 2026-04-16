#!/usr/bin/env bash
# Tags: no-fasttest

# Test that ProtobufList and Protobuf format behave nicely with their caches.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CURDIR/format_schemas
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -eo pipefail

FORMAT_SCHEMA="$SCHEMADIR/04061_protobuf_cache_with_envelope:NumberAndSquare"

$CLICKHOUSE_CLIENT <<EOF
DROP TABLE IF EXISTS squares_04061;
CREATE TABLE squares_04061 (number UInt32, square UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO squares_04061 VALUES (2, 4), (0, 0), (3, 9);
EOF

# Use ProtobufList BEFORE Protobuf has cached anything.
echo "ProtobufList before Protobuf (cold cache):"
BINARY_FILE_PATH=$(mktemp "$CURDIR/04061_protobuf_cache_with_envelope.XXXXXX.binary")
$CLICKHOUSE_CLIENT --query "SELECT * FROM squares_04061 ORDER BY number FORMAT ProtobufList SETTINGS format_schema = '$FORMAT_SCHEMA'" > "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "CREATE TABLE roundtrip1_04061 AS squares_04061"
$CLICKHOUSE_CLIENT --query "INSERT INTO roundtrip1_04061 SETTINGS format_schema = '$FORMAT_SCHEMA' FORMAT ProtobufList" < "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "SELECT * FROM roundtrip1_04061 ORDER BY number"
rm "$BINARY_FILE_PATH"

# Use Protobuf format to populate the cache for this schema.
echo "Protobuf:"
BINARY_FILE_PATH=$(mktemp "$CURDIR/04061_protobuf_cache_with_envelope.XXXXXX.binary")
$CLICKHOUSE_CLIENT --query "SELECT * FROM squares_04061 ORDER BY number FORMAT Protobuf SETTINGS format_schema = '$FORMAT_SCHEMA'" > "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "CREATE TABLE roundtrip2_04061 AS squares_04061"
$CLICKHOUSE_CLIENT --query "INSERT INTO roundtrip2_04061 SETTINGS format_schema = '$FORMAT_SCHEMA' FORMAT Protobuf" < "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "SELECT * FROM roundtrip2_04061 ORDER BY number"
rm "$BINARY_FILE_PATH"

# Use ProtobufList AFTER Protobuf has populated the cache.
echo "ProtobufList after Protobuf (warm cache):"
BINARY_FILE_PATH=$(mktemp "$CURDIR/04061_protobuf_cache_with_envelope.XXXXXX.binary")
$CLICKHOUSE_CLIENT --query "SELECT * FROM squares_04061 ORDER BY number FORMAT ProtobufList SETTINGS format_schema = '$FORMAT_SCHEMA'" > "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "CREATE TABLE roundtrip3_04061 AS squares_04061"
$CLICKHOUSE_CLIENT --query "INSERT INTO roundtrip3_04061 SETTINGS format_schema = '$FORMAT_SCHEMA' FORMAT ProtobufList" < "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "SELECT * FROM roundtrip3_04061 ORDER BY number"
rm "$BINARY_FILE_PATH"

$CLICKHOUSE_CLIENT <<EOF
DROP TABLE squares_04061;
DROP TABLE roundtrip1_04061;
DROP TABLE roundtrip2_04061;
DROP TABLE roundtrip3_04061;
EOF
