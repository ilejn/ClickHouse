import os
from minio import Minio
import clickhouse_connect
from optparse import OptionParser
import urllib3


usage = """
%prog [options]
    s3 garbage collector for ClickHouse
    example: $ ./s3gc.py
"""

parser = OptionParser(usage=usage)

parser.add_option("--ch-host", "--chhost", dest="chhost", default="localhost",
    help="ClickHouse host to connect to")
parser.add_option("--ch-port", "--chport", dest="chport", default=8123,
    help="ClickHouse port to connect to")
parser.add_option("--ch-user-name", "--chusername", dest="chuser", default="default",
    help="ClickHouse user name")
parser.add_option("--ch-pass", "--ch-password", "--chpass", dest="chpass", default="",
    help="ClickHouse user password")
parser.add_option("--s3-ip", "--s3ip", dest="s3ip", default="127.0.0.1",
    help="s3 ip")
parser.add_option("--s3-port", "--s3port", dest="s3port", default=9001,
    help="s3 port")
parser.add_option("--s3-bucket", "--s3bucket", dest="s3bucket", default="root",
    help="s3 port")
parser.add_option("--s3-access-key", "--s3accesskey", dest="s3accesskey", default="127.0.0.1",
    help="s3 accesskey")
parser.add_option("--s3-secret-key", "--s3secretkey", dest="s3secretkey", default="127.0.0.1",
    help="s3 secret key")
parser.add_option("--s3-secure", "--s3secure", dest="s3secure", default=False,
    help="s3 secure")
parser.add_option("--s3-ssl-cert-file", "--s3sslcertfile", dest="s3sslcertfile", default="",
    help="s3 secure")
parser.add_option("--s3-disk-name", "--s3diskname", dest="s3diskname", default="s3",
    help="s3 disk name")

(options, args) = parser.parse_args()

ch_client = clickhouse_connect.get_client(host=options.chhost, port=options.chport, username=options.chuser, password=options.chpass)

if options.s3secure:
    os.environ["SSL_CERT_FILE"] = options.s3sslcertfile


minio_client = Minio(
    f"{options.s3ip}:{options.s3port}",
    access_key=options.s3accesskey,
    secret_key=options.s3secretkey,
    secure=options.s3secure,
    http_client=urllib3.PoolManager(cert_reqs="CERT_NONE"),
)  # disable SSL check as we test ClickHouse and not Python library


objects = minio_client.list_objects(options.s3bucket, "data/", recursive=True)
# print([obj.object_name for obj in objects])

tname = f"s3objects_for_{options.s3diskname}"

ch_client.command(f"CREATE TABLE IF NOT EXISTS {tname} (objpath String) ENGINE MergeTree ORDER BY objpath")

for obj in objects:
    ch_client.insert(tname, [[obj.object_name]], column_names=['objpath'])


# res = ch_client.query(f"""SELECT s3o.objpath FROM {tname} AS s3o
# LEFT ANTI JOIN system.remote_data_paths AS rdp ON rdp.remote_path = s3o.objpath
# WHERE rdp.disk_name={dname:String}
# """)
# print(res.column_names)

res = ch_client.query("SELECT * from system.remote_data_paths")
print(res.column_names)


# with ch_client.query_row_block_stream("SELECT remote_path from system.remote_data_paths where disk_name={dname:String}",
#                                       parameters={'dname':'s3'}) as stream:
with ch_client.query_row_block_stream(f"""
SELECT s3o.objpath FROM {tname} AS s3o LEFT ANTI JOIN system.remote_data_paths AS rdp ON rdp.remote_path = s3o.objpath
AND rdp.disk_name='{options.s3diskname}'
""") as stream:
    for block in stream:
        for row in block:
            print(row[0])
            minio_client.remove_object(options.s3bucket, row[0])

# for column_name in res.column_names:
#     print(column_name)

print("hello, s3 world!")
print("s3gc: OK")
