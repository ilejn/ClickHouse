#!/usr/bin/env python3

import time
import io
import pytest
from kazoo.client import KazooClient
from helpers.cluster import ClickHouseCluster, ClickHouseInstance
from helpers.network import PartitionManager


@pytest.fixture(scope="module")
def started_cluster(request):
    cluster = ClickHouseCluster(__file__)
    try:
        cluster.add_instance(
            "node",
            main_configs=["configs/config.xml"],
            with_zookeeper=True,
            with_minio=True,
        )
        cluster.add_instance(
            "node_metamorph",
            main_configs=["configs/non_vfs.xml"],
            with_zookeeper=True,
            with_minio=True,
            stay_alive=True,
        )
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


# TODO myrrc check possible errors on merge and move
def test_session_breaks(started_cluster):
    node: ClickHouseInstance = started_cluster.instances["node"]
    # non-replicated MergeTree implies ZK data flow will be vfs-related
    node.query("CREATE TABLE test (i UInt32) ENGINE=MergeTree ORDER BY i")
    node.query("INSERT INTO test VALUES (0)")

    with PartitionManager() as pm:
        pm.drop_instance_zk_connections(node)
        node.query_and_get_error("INSERT INTO test VALUES (1)")
        time.sleep(4)
    time.sleep(2)  # Wait for CH to reconnect to ZK before next GC run

    assert (
        int(node.count_in_log("VFSGC(reacquire): Removed lock for")) > 1
    ), "GC must run at least twice"
    assert (
        int(node.count_in_log("Trying to establish a new connection with ZooKeeper"))
        > 1
    ), "ZooKeeper session must expire"

    node.query("INSERT INTO test VALUES (2)")
    assert int(node.query("SELECT count() FROM test")) == 2
    node.query("DROP TABLE test")


def test_reconcile(started_cluster):
    node: ClickHouseInstance = started_cluster.instances["node"]
    while int(node.count_in_log("VFSGC(reconcile): Removed lock for")) < 1:
        time.sleep(0.3)

    # Already processed log batch

    zk: KazooClient = started_cluster.get_kazoo_client("zoo1")
    disk_prefix: str = "/vfs/reconcile"
    assert zk.get_children(f"{disk_prefix}/ops") == []
    for i in range(11, 15):  # snapshot for 10 doesn't exist
        zk.create(f"{disk_prefix}/ops/log-00000000{i}", b"")

    started_cluster.minio_client.put_object(
        started_cluster.minio_bucket,
        "data/vfs/reconcile/snapshots/14",  # last written log item is 14
        io.StringIO(""),
        0,
    )

    time.sleep(5)
    assert int(node.count_in_log("Found leftover from previous GC run")) == 1
    assert zk.get_children(f"{disk_prefix}/ops") == []

    # Zookeeper full loss -- node counter resets but snapshot present

    zk.delete(disk_prefix, recursive=True)
    for i in range(5):  # we have leftover snapshot 1 and 14 but not 0
        zk.create(f"{disk_prefix}/ops/log-", b"", sequence=True, makepath=True)
    time.sleep(11)  # GC will fail, reschedule, and then succeed
    assert int(node.count_in_log("Selected snapshot 14 as best candidate")) == 1

    # Log part loss -- select best snapshot from past

    for i in range(11, 15):  # we have snapshot for 1 and 4
        zk.create(f"{disk_prefix}/ops/log-00000000{i}", b"")
    time.sleep(6)
    assert int(node.count_in_log("Selected snapshot 4 as best candidate")) == 1

    zk.stop()

def test_change_disk_flavor_from_vfs(started_cluster):
    vfs_config =  """<clickhouse>
    <storage_configuration>
        <disks>
            <s3>
                <type>s3</type>
                <allow_vfs>true</allow_vfs>
                <vfs_gc_sleep_ms>5000</vfs_gc_sleep_ms>
                <endpoint>http://minio1:9001/root/morph/</endpoint>
                <access_key_id>minio</access_key_id>
                <secret_access_key>minio123</secret_access_key>
            </s3>
        </disks>
        <policies>
            <default>
                <volumes>
                    <main>
                        <disk>s3</disk>
                    </main>
                </volumes>
            </default>
        </policies>
    </storage_configuration>
</clickhouse>
"""

    no_vfs_config =  """<clickhouse>
    <storage_configuration>
        <disks>
            <s3>
                <type>s3</type>
                <allow_vfs>false</allow_vfs>
                <endpoint>http://minio1:9001/root/morph/</endpoint>
                <access_key_id>minio</access_key_id>
                <secret_access_key>minio123</secret_access_key>
            </s3>
        </disks>
        <policies>
            <default>
                <volumes>
                    <main>
                        <disk>s3</disk>
                    </main>
                </volumes>
            </default>
        </policies>
    </storage_configuration>
</clickhouse>
"""
    config_file = "/etc/clickhouse-server/config.d/vfs.xml"

    node: ClickHouseInstance = started_cluster.instances["node_metamorph"]

    node.replace_config(config_file, vfs_config, )
    node.restart_clickhouse()

    node.query("CREATE TABLE mtest (i UInt32) ENGINE=MergeTree ORDER BY i")
    node.query("INSERT INTO mtest VALUES (0)")

    assert int(node.query("SELECT count() FROM mtest")) == 1


    node.stop_clickhouse()
    node.replace_config(config_file, no_vfs_config, )

    node.start_clickhouse(retry_start=False, expected_to_fail=True)
    time.sleep(3)
    assert node.contains_in_log("DB::Exception: Attempt to use VFS disk as non-VFS")
