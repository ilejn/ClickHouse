"""Tests for Avro virtual columns with Kafka engine."""

from helpers.kafka.common_direct import *
import helpers.kafka.common as k


cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/kafka.xml"],
    user_configs=["configs/users.xml"],
    with_kafka=True,
    with_zookeeper=True,
    stay_alive=True,
    macros={
        "kafka_broker": "kafka1",
        "kafka_format_json_each_row": "JSONEachRow",
    },
    clickhouse_path_dir="clickhouse_path",
)


@pytest.fixture(scope="module")
def kafka_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def kafka_setup_teardown():
    k.clean_test_database_and_topics(instance, cluster)
    yield


def make_avro_union_message(value):
    schema = avro.schema.make_avsc_object(
        {
            "name": "row",
            "type": "record",
            "fields": [
                {"name": "id", "type": "int"},
                {
                    "name": "nullable_payload",
                    "type": [
                        "null",
                        {
                            "type": "record",
                            "name": "TypeA",
                            "fields": [{"name": "x", "type": "int"}],
                        },
                    ],
                },
            ],
        }
    )

    bytes_writer = io.BytesIO()
    writer = avro.datafile.DataFileWriter(bytes_writer, avro.io.DatumWriter(), schema)
    writer.append(value)
    writer.flush()
    raw_bytes = bytes_writer.getvalue()
    writer.close()
    bytes_writer.close()
    return raw_bytes


def test_kafka_avro_union_type_name_virtual_column(kafka_cluster):
    topic_name = f"avro_union_virtual_{k.random_string(6)}"
    kafka_table = f"kafka_avro_union_virtual_{k.random_string(6)}"
    mv_table = f"kafka_avro_union_virtual_dst_{k.random_string(6)}"
    mv_name = f"{kafka_table}_mv"

    messages = [
        make_avro_union_message({"id": 1, "nullable_payload": None}),
        make_avro_union_message({"id": 2, "nullable_payload": {"x": 10}}),
    ]

    with k.kafka_topic(k.get_admin_client(kafka_cluster), topic_name):
        create_query = k.generate_new_create_table_query(
            kafka_table,
            "id Int32, `nullable_payload.TypeA.x` Nullable(Int32)",
            topic_list=topic_name,
            consumer_group=f"{topic_name}_group",
            format="Avro",
            settings={
                "input_format_avro_union_type_name": 1,
                "kafka_flush_interval_ms": 1000,
            },
        )
        instance.query(create_query)

        instance.query(f"""
            CREATE TABLE test.{mv_table}
            (
                id Int32,
                nullable_payload_x Nullable(Int32),
                nullable_payload_name Nullable(String)
            )
            ENGINE = MergeTree
            ORDER BY id
        """)

        instance.query(f"""
            CREATE MATERIALIZED VIEW test.{mv_name}
            TO test.{mv_table}
            AS
            SELECT
                id,
                `nullable_payload.TypeA.x` AS nullable_payload_x,
                `nullable_payload.$name` AS nullable_payload_name
            FROM test.{kafka_table}
        """)

        k.kafka_produce(kafka_cluster, topic_name, messages)

        expected = TSV(
            """
1\t\\N\t\\N
2\t10\tTypeA
"""
        )

        result = instance.query_with_retry(
            f"SELECT id, nullable_payload_x, nullable_payload_name FROM test.{mv_table} ORDER BY id",
            check_callback=lambda res: TSV(res) == expected,
            retry_count=30,
            sleep_time=1,
        )

        assert TSV(result) == expected
