import avro.datafile
import avro.io
import avro.schema

schema = avro.schema.parse(
    """
    {
      "type": "record",
      "name": "Root",
      "fields": [
        {"name": "id", "type": "int"},
        {"name": "outer",
         "type": {
           "type": "record",
           "name": "Outer",
           "fields": [
             {"name": "inner",
              "type": ["null",
                       {"type": "record", "name": "InnerType",
                        "fields": [{"name": "x", "type": "int"}]}]}
           ]
         }}
      ]
    }
    """
)

records = [
    {"id": 1, "outer": {"inner": None}},
    {"id": 2, "outer": {"inner": {"x": 7}}},
    {"id": 3, "outer": {"inner": {"x": 11}}},
]

with open("nested_union_type_name.avro", "wb") as f:
    writer = avro.datafile.DataFileWriter(f, avro.io.DatumWriter(), schema)
    for record in records:
        writer.append(record)
    writer.close()
