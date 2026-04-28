#!/usr/bin/env bash
# Tags: no-fasttest

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR=$CUR_DIR/data_avro

file_name="$CLICKHOUSE_DATABASE"_union_type_name_branch_subcolumn.avro
cp "$DATA_DIR/union_type_name.avro" "$CLICKHOUSE_USER_FILES/$file_name"

echo "== Explicit schema: branch payload column without union \$name =="
$CLICKHOUSE_CLIENT -q "
  SELECT id, \`nullable_payload.TypeA.x\`
  FROM file('$file_name', 'Avro', '
    id Int32,
    \`nullable_payload.TypeA.x\` Nullable(Int32)
  ')
  ORDER BY id
"
echo

echo "== Explicit schema: union \$name together with branch payload column =="
$CLICKHOUSE_CLIENT -q "
  SELECT id, \`nullable_payload.\$name\`, \`nullable_payload.TypeA.x\`
  FROM file('$file_name', 'Avro', '
    id Int32,
    \`nullable_payload.\$name\` Nullable(String),
    \`nullable_payload.TypeA.x\` Nullable(Int32)
  ')
  ORDER BY id
" 2>&1
