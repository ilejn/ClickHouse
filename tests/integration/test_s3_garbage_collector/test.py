#!/usr/bin/env python3

import logging
import random
import string
import time
import os

import pytest
from helpers.cluster import ClickHouseCluster
import minio


cluster = ClickHouseCluster(__file__)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.add_instance(
            "node1",
            main_configs=["configs/storage_conf.xml"],
            with_minio=True,
            with_zookeeper=True,
        )
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def get_objects_in_data_path():
    minio = cluster.minio_client
    objects = minio.list_objects(cluster.minio_bucket, "data/", recursive=True)
    return [obj.object_name for obj in objects]


def make_object_in_data_path():
    minio = cluster.minio_client
    minio.fput_object(cluster.minio_bucket, "data/fakeobject", "/etc/hosts")


def test_s3_nr(started_cluster):
    node1 = cluster.instances["node1"]

    # objects_before = get_objects_in_data_path()

    node1.query(
        """
CREATE TABLE test_s3_nr(c1 Int8, c2 Date) ENGINE = MergeTree() PARTITION BY c2 ORDER BY c2
SETTINGS storage_policy = 's3'
        """
    )
    node1.query("INSERT INTO test_s3_nr VALUES (1, '2023-10-04'), (2, '2023-10-04')")
    assert node1.query("SELECT count() FROM test_s3_nr") == "2\n"

    SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

    node1.copy_file_to_container(os.path.join(SCRIPT_DIR, "s3gc.py"), "/s3gc.py")
    node1.exec_in_container(["pip3", "install", "clickhouse_connect"], user="root")
    node1.exec_in_container(["pip3", "install", "minio"], user="root")
    node1.exec_in_container(
        [
            "bash",
            "-c",
            f"python3 /s3gc.py --s3-ip={cluster.minio_ip} --s3-bucket={cluster.minio_bucket} --s3-access-key=minio --s3-secret-key=minio123 --s3-ssl-cert-file=/public.crt --debug > /s3gc.log 2>&1",
        ],
        detach=True,
        user="root",
    )

    objects_before = get_objects_in_data_path()
    make_object_in_data_path()  # fake object

    result = None
    for attempt in range(1, 6):
        time.sleep(attempt)
        result = node1.exec_in_container(["cat", "/s3gc.log"], user="root")
        result_lines = result.splitlines()
        result_status = "UNKNOWN"
        if len(result_lines):
            result_status = result_lines[len(result_lines) - 1]

        if result_status == "s3gc: OK":
            break
        if result_status == "s3gc: FAIL":
            break
    print(result)
    objects_after = get_objects_in_data_path()

    node1.query("ALTER TABLE test_s3_nr DETACH PARTITION '2023-10-04'")

    node1.query(
        "ALTER TABLE test_s3_nr DROP DETACHED PARTITION '2023-10-04' SETTINGS allow_drop_detached = 1"
    )

    assert objects_before == objects_after
