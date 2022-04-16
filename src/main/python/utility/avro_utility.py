from typing import Any, Dict

from google.pubsub_v1 import Encoding


class AvroUtility:

    @classmethod
    def deserialize(cls, record, schema: Dict[str, Any], encoding: Encoding) -> Dict:
        if encoding is Encoding.BINARY:
            return cls.__deserialize_binary(record, schema)

        if encoding is Encoding.JSON:
            return cls.__deserialize_json(record, schema)

        raise TypeError('No encoding method is available for this type of encoding.', encoding)
