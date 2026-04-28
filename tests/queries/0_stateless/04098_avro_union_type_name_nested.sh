#!/usr/bin/env bash
# Tags: no-fasttest

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR=$CUR_DIR/data_avro

file_name="$CLICKHOUSE_DATABASE"_nested_union_type_name.avro
cp "$DATA_DIR/nested_union_type_name.avro" "$CLICKHOUSE_USER_FILES/$file_name"

echo "== DESCRIBE with union_type_name enabled for nested union =="
$CLICKHOUSE_CLIENT -q "DESCRIBE file('$file_name') SETTINGS input_format_avro_union_type_name=1"
echo

echo "== Explicit schema: only nested \$name column =="
$CLICKHOUSE_CLIENT -q "
  SELECT id, \`outer.inner.\$name\`
  FROM file('$file_name', 'Avro', '
    id Int32,
    \`outer.inner.\$name\` Nullable(String)
  ')
  ORDER BY id
"
echo
