#!/usr/bin/env python3

import time
import io
import os
import pytest
from kazoo.client import KazooClient
from helpers.cluster import ClickHouseCluster, ClickHouseInstance
from helpers.network import PartitionManager
from concurrent.futures import ThreadPoolExecutor, wait

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

# os.path.join
CONF_DIR = "{}/configs/".format(os.path.dirname(os.path.abspath(__file__)))

misstep_vfs_config =  """<clickhouse>
    <storage_configuration>
        <disks>
            <{0}>
                <type>s3</type>
                <allow_vfs>true</allow_vfs>
                <vfs_gc_sleep_ms>5000</vfs_gc_sleep_ms>
                <endpoint>http://minio1:9001/root/{0}/</endpoint>
                <access_key_id>minio</access_key_id>
                <secret_access_key>minio123</secret_access_key>
            </{0}>
        </disks>
        <policies>
            <s3_policy>
                <volumes>
                    <main>
                        <disk>{0}</disk>
                    </main>
                </volumes>
            </s3_policy>
        </policies>
    </storage_configuration>
</clickhouse>
"""

misstep_non_vfs_config =  """<clickhouse>
    <storage_configuration>
        <disks>
            <{0}>
                <type>s3</type>
                <allow_vfs>false</allow_vfs>
                <endpoint>http://minio1:9001/root/{0}/</endpoint>
                <access_key_id>minio</access_key_id>
                <secret_access_key>minio123</secret_access_key>
            </{0}>
        </disks>
        <policies>
            <s3_policy>
                <volumes>
                    <main>
                        <disk>{0}</disk>
                    </main>
                </volumes>
            </s3_policy>
        </policies>
    </storage_configuration>
</clickhouse>
"""



# TODO myrrc dimension for 0copy
# TODO myrrc dimension for large data
@pytest.fixture(scope="module", params=[False, True], ids=["sequential", "parallel"])
def started_cluster(request):
    with open(CONF_DIR + "misstep_non_vfs.xml", "w+") as f:
        f.write(misstep_non_vfs_config.format("misstep_to_vfs"))
    with open(CONF_DIR + "misstep_vfs.xml", "w+") as f:
        f.write(misstep_vfs_config.format("misstep_from_vfs"))
    cluster = ClickHouseCluster(__file__)
    try:
        for i in range(1, 4):
            cluster.add_instance(
                f"node{i}",
                main_configs=["configs/config.xml"],
                with_zookeeper=True,
                with_minio=True,
                macros={"replica": f"node{i}"},
                stay_alive=True,
            )
        cluster.add_instance(
            "node_to_vfs",
            main_configs=["configs/misstep_non_vfs.xml"],
            with_zookeeper=True,
            with_minio=True,
            stay_alive=True,
        )
        cluster.add_instance(
            "node_from_vfs",
            main_configs=["configs/misstep_vfs.xml"],
            with_zookeeper=True,
            with_minio=True,
            stay_alive=True,
        )

        cluster.start()
        print("Cluster started")

        yield cluster, request.param
    finally:
        cluster.shutdown()


def test_to_vfs(started_cluster):
    cluster, parallel = started_cluster
    node1: ClickHouseInstance = cluster.instances["node1"]

    node1.query(
        "CREATE TABLE test ON CLUSTER cluster (i UInt32, t UInt32 DEFAULT i) "
        "ENGINE=ReplicatedMergeTree('/clickhouse/tables/test', '{replica}') "
        "ORDER BY i PARTITION BY i % 10"
    )
    node1.query("INSERT INTO test (i) SELECT * FROM numbers(2)")
    node1.query("ALTER TABLE test UPDATE t = t + 5 WHERE i > 0")
    node1.query("INSERT INTO test (i) SELECT * FROM numbers(2, 1)")

    def validate(node, a, b):
        assert node.query("SELECT count(), uniqExact(t) FROM test") == f"{a}\t{b}\n"

    validate(node1, 3, 3)

    def migrate(i):
        print(f"Migrating node {i}")
        node: ClickHouseInstance = cluster.instances[f"node{i}"]
        node.query("SYSTEM SYNC REPLICA test")
        node.copy_file_to_container(
            os.path.join(SCRIPT_DIR, f"configs/vfs.xml"),
            "/etc/clickhouse-server/config.d/vfs.xml",
        )
        node.restart_clickhouse()
        assert node.contains_in_log("Migrated disk s3")
        node.query(f"INSERT INTO test (i) SELECT * FROM numbers(3 + {i}, 1)")

    with ThreadPoolExecutor(max_workers=3 if parallel else 1) as exe:
        exe.map(migrate, range(1, 4))
        exe.shutdown(wait=True)

    for i in range(1, 4):
        node: ClickHouseInstance = cluster.instances[f"node{i}"]
        node.query("SYSTEM SYNC REPLICA test")
        validate(node, 6, 5)

    node1.query("DROP TABLE test ON CLUSTER cluster SYNC")


def test_misstep_from_vfs(started_cluster):
    cluster, parallel = started_cluster
    config_file = "/etc/clickhouse-server/config.d/misstep_vfs.xml"

    node: ClickHouseInstance = cluster.instances["node_from_vfs"]

    # node.replace_config(config_file, vfs_config, )
    # node.restart_clickhouse()

    node.query("CREATE TABLE mtest (i UInt32) ENGINE=MergeTree ORDER BY i SETTINGS storage_policy='s3_policy'")
    node.query("INSERT INTO mtest VALUES (0)")

    assert int(node.query("SELECT count() FROM mtest")) == 1


    node.stop_clickhouse()
    node.replace_config(config_file, misstep_non_vfs_config.format('misstep_from_vfs'), )

    node.start_clickhouse(retry_start=False, expected_to_fail=True)
    time.sleep(3)
    assert node.contains_in_log("DB::Exception: Attempt to use VFS disk as non-VFS")


def test_misstep_to_vfs(started_cluster):
    cluster, parallel = started_cluster
    config_file = "/etc/clickhouse-server/config.d/misstep_non_vfs.xml"

    node: ClickHouseInstance = cluster.instances["node_to_vfs"]



    # node.replace_config(config_file, vfs_config.format('to_vfs'), )
    # node.restart_clickhouse()

    node.query("CREATE TABLE mtest (i UInt32) ENGINE=MergeTree ORDER BY i SETTINGS storage_policy='s3_policy'")
    node.query("INSERT INTO mtest VALUES (0)")

    assert int(node.query("SELECT count() FROM mtest")) == 1

    time.sleep(20)

    node.stop_clickhouse()
    node.replace_config(config_file, misstep_vfs_config.format('misstep_to_vfs'), )
    # node.start_clickhouse()

    # assert 2 == 1


    node.start_clickhouse(retry_start=False, expected_to_fail=True)
    time.sleep(3)

    found = False;
    for a_node_name in cluster.instances:
        a_node : ClickHouseInstance = cluster.instances[a_node_name]
        if a_node.contains_in_log("DB::Exception: Attempt to use non-VFS disk as VFS"):
            found = True

    assert found
    # assert node.contains_in_log("<Error> VFSGC(s3): Didn't find any snapshots in misstep_to_vfs/vfs/s3/snapshots")
