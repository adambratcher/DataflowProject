from io import StringIO
from json import dumps
from typing import Any, Dict

from fastavro import json_reader, json_writer, reader


class AvroService:

    def __init__(self, schema: Dict[str, Any]):
        self.schema: Dict[str, Any] = schema

    def deserialize(self, record: str) -> Dict[str, Any]:
        return self.deserialize_json(record)

    def deserialize_json(self, record: str) -> Dict[str, Any]:
        string_reader: StringIO = StringIO(record)
        print(string_reader.getvalue())
        avro_reader: reader = json_reader(string_reader, self.schema)

        return avro_reader.next()

    def serialize(self, record: str) -> bytes:
        return self.serialize_json(record)

    def serialize_json(self, record: str) -> bytes:
        string_writer: StringIO = StringIO()

        json_writer(string_writer, self.schema, record)

        return string_writer.getvalue().encode('utf-8')


schema: Dict[str, Any] = {
    "type": "record",
    "name": "Organization",
    "fields": [
        {
            "name": "event_name",
            "type": "string"
        },
        {
            "name": "event_version",
            "type": "string"
        },
        {
            "name": "feedback_id",
            "type": "int"
        }
    ]
}

record: str = '''{
    "event_name": "name1",
    "event_version": "version1",
    "feedback_id": 0
}
'''
record2: str = '''{"event_name":      \t  
  "name1", "event_version": "version1", "feedbac
  k_id": 0}'''

cleaned_record: str = dumps(record2)
print(cleaned_record)

avro_service: AvroService = AvroService(schema)

avro_service.deserialize(cleaned_record)
