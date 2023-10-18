#!/usr/bin/env python3

import logging
import random
import string
import time

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

    node1.query(
        """
CREATE TABLE test_s3_nr(c1 Int8, c2 Date) ENGINE = MergeTree() PARTITION BY c2 ORDER BY c2
        """
    )

    node1.query("INSERT INTO test_s3_nr VALUES (1, '2023-10-04'), (2, '2023-10-04')")

    assert node1.query("SELECT count() FROM test_s3_nr") == "2\n"




    objects_before = get_objects_in_data_path()
    make_object_in_data_path()  # fake object

    node1.query(
        "ALTER TABLE test_s3_nr DETACH PARTITION '2023-10-04'"
    )


    node1.query(
        "ALTER TABLE test_s3_nr DROP DETACHED PARTITION '2023-10-04' SETTINGS allow_drop_detached = 1"
    )

    objects_after = get_objects_in_data_path()

    # assertion should be failed because of fake object
    assert objects_before == objects_after
