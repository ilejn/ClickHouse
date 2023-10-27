import os
from minio import Minio
import clickhouse_connect
from optparse import OptionParser
import urllib3
import logging

usage = """
%prog [options]
    s3 garbage collector for ClickHouse
    example: $ ./s3gc.py
"""

parser = OptionParser(usage=usage)

parser.add_option(
    "--ch-host",
    "--chhost",
    dest="chhost",
    default="localhost",
    help="ClickHouse host to connect to",
)
parser.add_option(
    "--ch-port",
    "--chport",
    dest="chport",
    default=8123,
    help="ClickHouse port to connect to",
)
parser.add_option(
    "--ch-user-name",
    "--chusername",
    dest="chuser",
    default="default",
    help="ClickHouse user name",
)
parser.add_option(
    "--ch-pass",
    "--ch-password",
    "--chpass",
    dest="chpass",
    default="",
    help="ClickHouse user password",
)
parser.add_option(
    "--s3-ip", "--s3ip", dest="s3ip", default="127.0.0.1", help="S3 ip address"
)
parser.add_option(
    "--s3-port", "--s3port", dest="s3port", default=9001, help="S3 API port"
)
parser.add_option(
    "--s3-bucket", "--s3bucket", dest="s3bucket", default="root", help="S3 bucker name"
)
parser.add_option(
    "--s3-access-key",
    "--s3accesskey",
    dest="s3accesskey",
    default="127.0.0.1",
    help="S3 access key",
)
parser.add_option(
    "--s3-secret-key",
    "--s3secretkey",
    dest="s3secretkey",
    default="127.0.0.1",
    help="S3 secret key",
)
parser.add_option(
    "--s3-secure", "--s3secure", dest="s3secure", default=False, help="S3 secure mode"
)
parser.add_option(
    "--s3-ssl-cert-file",
    "--s3sslcertfile",
    dest="s3sslcertfile",
    default="",
    help="SSL certificate for S3",
)
parser.add_option(
    "--s3-disk-name",
    "--s3diskname",
    dest="s3diskname",
    default="s3",
    help="S3 disk name",
)
parser.add_option(
    "--keep-data",
    "--keepdata",
    action="store_true",
    dest="keepdata",
    default=False,
    help="keep auxiliary data in ClickHouse table",
)
parser.add_option(
    "--collect-only",
    "--collectonly",
    action="store_true",
    dest="collectonly",
    default=False,
    help="keep auxiliary data in ClickHouse table",
)
parser.add_option(
    "--use-collected",
    "--usecollected",
    action="store_true",
    dest="usecollected",
    default=False,
    help="auxiliary data is already collected in ClickHouse table",
)
parser.add_option(
    "--collect-table-prefix",
    "--collecttableprefix",
    dest="collecttableprefix",
    default="s3objects_for_",
    help="prefix for table name to keep data about objects (tablespace is allowed)",
)
parser.add_option(
    "--batch-size",
    "--batchsize",
    dest="batchsize",
    default=1024,
    help="number of rows to insert to ClickHouse at once",
)
parser.add_option(
    "--cluster",
    "--cluster-name",
    "--clustername",
    dest="clustername",
    default="",
    help="consider an objects unused if there is no host in the cluster refers the object",
)

parser.add_option(
    "--verbose", action="store_true", dest="verbose", default=False, help="debug output"
)
parser.add_option(
    "--debug",
    action="store_true",
    dest="debug",
    default=False,
    help="trace output (more verbose)",
)
parser.add_option(
    "--silent", action="store_true", dest="silent", default=False, help="no log"
)

(options, args) = parser.parse_args()

logging.getLogger().setLevel(logging.WARNING)
if options.verbose:
    logging.getLogger().setLevel(logging.INFO)
if options.debug:
    logging.getLogger().setLevel(logging.DEBUG)
if options.silent:
    logging.getLogger().setLevel(logging.CRITICAL)

logging.info("aaa")

logging.info(
    f"Connecting to ClickHouse, host={options.chhost}, port={options.chport}, username={options.chuser}, password={options.chpass}"
)
ch_client = clickhouse_connect.get_client(
    host=options.chhost,
    port=options.chport,
    username=options.chuser,
    password=options.chpass,
)

if options.s3secure:
    logging.debug(f"using SSL certificate {options.s3sslcertfile}")
    os.environ["SSL_CERT_FILE"] = options.s3sslcertfile

tname = f"{options.collecttableprefix}{options.s3diskname}"

if not options.usecollected:
    logging.info(
        f"Connecting to S3, host:port={options.s3ip}:{options.s3port}, access_key={options.s3accesskey}, secret_key={options.s3secretkey}, secure={options.s3secure}"
    )
    minio_client = Minio(
        f"{options.s3ip}:{options.s3port}",
        access_key=options.s3accesskey,
        secret_key=options.s3secretkey,
        secure=options.s3secure,
        http_client=urllib3.PoolManager(cert_reqs="CERT_NONE"),
    )

    objects = minio_client.list_objects(options.s3bucket, "data/", recursive=True)

    logging.info(f"creating {tname}")
    ch_client.command(
        f"CREATE TABLE IF NOT EXISTS {tname} (objpath String) ENGINE MergeTree ORDER BY objpath"
    )
    go_on = True
    while go_on:
        objs = []
        for batch_element in range(1, options.batchsize):
            try:
                objs.append([next(objects).object_name])
            except StopIteration:
                go_on = False
        ch_client.insert(tname, objs, column_names=["objpath"])

if not options.collectonly:
    srdp = "system.remote_data_paths"
    if options.clustername:
        srdp = f"clusterAllReplicas({options.clustername}, {srdp})"

    antijoin = f"""
    SELECT s3o.objpath FROM {tname} AS s3o LEFT ANTI JOIN {srdp} AS rdp ON rdp.remote_path = s3o.objpath
    AND rdp.disk_name='{options.s3diskname}'
    """
    logging.info(antijoin)

    with ch_client.query_row_block_stream(antijoin) as stream:
        for block in stream:
            for row in block:
                logging.debug(row[0])
                minio_client.remove_object(options.s3bucket, row[0])

    if not options.keepdata:
        logging.info(f"truncating {tname}")
        ch_client.command(f"TRUNCATE TABLE {tname}")

if not options.silent:
    print("s3gc: OK")
