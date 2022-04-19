import logging
from argparse import ArgumentParser
from datetime import datetime
from io import StringIO
from json import loads
from typing import Any, Dict, Hashable, List, Sequence, Tuple, Union

from apache_beam import DoFn, Map, Pipeline
from apache_beam.io import BigQueryDisposition, ReadFromPubSub, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from fastavro import json_reader, parse_schema, reader
from google.api_core.exceptions import NotFound
from google.pubsub_v1 import Encoding, GetSubscriptionRequest, PublisherClient, Schema, SchemaServiceClient, \
    SubscriberClient, \
    Subscription, Topic

logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(name)s : %(message)s", level=logging.INFO
)
logging = logging.getLogger(__name__)

raw_schema = {
    "type": "record",
    "namespace": "AvroPubSubDemo",
    "name": "Entity",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "name", "type": "string"},
        {"name": "dob", "type": "string"},
    ],
}

avro_type_to_bigquery_type_map: Dict[Tuple[str, Union[str, None]], str] = {
    ('boolean', None): 'BOOLEAN',
    ('int', None): 'INTEGER',
    ('long', None): 'INTEGER',
    ('float', None): 'FLOAT',
    ('double', None): 'FLOAT',
    ('bytes', None): 'BYTES',
    ('string', None): 'STRING',
    ('int', 'date'): 'DATE',
    ('int', 'time-millis'): 'TIME',
    ('long', 'time-micros'): 'TIME',
    ('long', 'timestamp-millis'): 'TIMESTAMP',
    ('long', 'timestamp-micros'): 'TIMESTAMP',
    ('string', 'uuid'): 'STRING',
    ('record', None): 'RECORD'
}


def get_bigquery_schema(avro_schema: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    return {'fields': get_bigquery_record_field_definitions(avro_schema)}


def get_bigquery_field_mode(field: Dict[str, Any]) -> str:
    if 'null' in field['type']:
        return 'NULLABLE'

    if field['type'] == 'array':
        return 'REPEATED'

    return 'REQUIRED'


def get_bigquery_field_nested_type(nested_field_type: Dict[str, Any]) -> str:
    if 'type' not in nested_field_type:
        raise KeyError('The "type" key was not found in nested type field.', nested_field_type)

    if nested_field_type['type'] == 'array':
        if 'items' not in nested_field_type:
            raise KeyError('The "items" key was not found in array type field.', nested_field_type)

        return get_bigquery_field_type(nested_field_type['items'])

    return get_bigquery_field_type(nested_field_type['type'])


def get_bigquery_union_field_type(union_field_type: List[Union[str, Dict[str, Any]]]) -> str:
    non_null_union_field_types: List[Union[str, Dict[str, Any]]] = \
        [field_type for field_type in union_field_type if field_type != 'null']

    if len(non_null_union_field_types) > 1:
        raise ValueError('Union types are not supported in BigQuery.', union_field_type)

    return get_bigquery_field_type(non_null_union_field_types[0])


def get_bigquery_field_type(field_type: Union[str, Dict[str, Any], List[Union[str, Dict[str, Any]]]]) -> str:
    if isinstance(field_type, list):
        return get_bigquery_union_field_type(field_type)

    if isinstance(field_type, dict):
        type_value: Union[str, Dict[str, Any], List[Union[str, Dict[str, Any]]]] = field_type.get('type')
        logical_type_value: Union[str, Dict[str, Any], List[Union[str, Dict[str, Any]]]] = field_type.get('logicalType')

        if isinstance(type_value, Hashable) and isinstance(logical_type_value, Hashable) \
                and (type_value, logical_type_value) in avro_type_to_bigquery_type_map:
            return avro_type_to_bigquery_type_map[(type_value, logical_type_value)]

        return get_bigquery_field_nested_type(field_type)

    if isinstance(field_type, str) and (field_type, None) in avro_type_to_bigquery_type_map:
        return avro_type_to_bigquery_type_map[(field_type, None)]

    raise TypeError('A BigQuery type could not be resolved for this field.', field_type)


def get_bigquery_field_definition(field: Dict[str, Any]) -> Dict[str, Any]:
    bigquery_field_definition: Dict[str, Any] = {}

    if 'name' not in field:
        raise KeyError('The "name" key was not found in the Avro field definition.', field)

    if 'type' not in field:
        raise KeyError('The "type" key was not found in the Avro field definition.', field)

    bigquery_field_definition['name'] = field['name']
    bigquery_field_definition['type'] = get_bigquery_field_type(field['type'])
    bigquery_field_definition['mode'] = get_bigquery_field_mode(field)

    if bigquery_field_definition['type'] == 'RECORD':
        bigquery_field_definition['fields'] = get_bigquery_record_field_definitions(field)

    return bigquery_field_definition


def get_bigquery_record_field_definitions(avro_schema: Dict[str, Any]) -> List[Dict[str, Any]]:
    next_avro_record_fields: List[Dict[str, Any]] = find_next_avro_record_fields(avro_schema)

    if not next_avro_record_fields:
        raise ValueError('Avro record fields could not be found.', avro_schema)

    return [get_bigquery_field_definition(field) for field in next_avro_record_fields]


def find_next_avro_record_fields(avro_schema: Dict[str, Any]) -> List[Dict[str, Any]]:
    if 'fields' in avro_schema:
        return avro_schema['fields']

    if 'type' in avro_schema:
        if isinstance(avro_schema['type'], dict):
            return find_next_avro_record_fields(avro_schema['type'])

        if isinstance(avro_schema['type'], list):
            return find_next_avro_record_fields(
                [field_type for field_type in avro_schema['type'] if field_type != 'null'][0])

    if 'items' in avro_schema:
        return find_next_avro_record_fields(avro_schema['items'])

    return []


def deserialize_avro(record: str, schema: Dict[str, Any], encoding: Encoding):
    if encoding is Encoding.JSON:
        return deserialize_json_avro(record, schema)

    if encoding is Encoding.BINARY:
        raise NotImplementedError('Avro binary deserialization has not been implemented yet.')

    raise TypeError('No deserialization method is available for this type of encoding.', encoding)


def deserialize_json_avro(record: str, schema: Dict[str, Any]) -> Dict[str, Any]:
    string_reader: StringIO = StringIO(record)

    avro_reader: reader = json_reader(string_reader, schema)

    return avro_reader[0]


# class avroReadWrite:
#     def __init__(self, schema):
#         self.schema = schema
#
#     def deserialize(self, record):
#         bytes_reader = BytesIO(record)
#         dict_record = schemaless_reader(bytes_reader, self.schema)
#         return dict_record
#
#     def serialize(self, record):
#         bytes_writer = BytesIO()
#         schemaless_writer(bytes_writer, self.schema, record)
#         bytes_array = bytes_writer.getvalue()
#         return bytes_array


class DataflowPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser: ArgumentParser):
        parser.add_argument('--input_subscription', required=True, help='pubsub input topic')
        parser.add_argument('--output_table', required=True, help='bigquery output table')
        parser.add_argument('--use_avro_logical_types', required=False, default=True, help='avro logical types')


def get_dataflow_pipeline_options() -> DataflowPipelineOptions:
    parser: ArgumentParser = ArgumentParser()

    pipeline_options = PipelineOptions(parser.parse_known_args()[1])

    pipeline_options.view_as(SetupOptions).save_main_session = True

    return pipeline_options.view_as(DataflowPipelineOptions)


class TransformerDoFn(DoFn):
    def __init__(self, _schema):
        self.schema = _schema

    def process(self, record):
        try:
            logging.info("-----------------------------------------------------------")
            logging.info("Input Record: {}".format(record))
            record["dob"] = str(
                datetime.strptime(record["dob"], "%d/%m/%Y").date()
            )
            logging.info("Output Record: {}".format(record))
            logging.info("-----------------------------------------------------------")
            return [record]
        except Exception as e:
            logging.error("Got Exceptons {}", format(e), exc_info=True)
            return [record]


def get_subscription(subscription_path: str) -> Subscription:
    try:
        return SubscriberClient().get_subscription(request=GetSubscriptionRequest({'subscription': subscription_path}))
    except NotFound:
        raise NotFound(f'Subscription not found: "{subscription_path}".')


def get_topic(topic_path: str) -> Topic:
    try:
        return PublisherClient().get_topic(request={"topic": topic_path})
    except NotFound:
        raise NotFound(f'Topic not found: "{topic_path}".')


def get_schema(schema_path: str) -> Dict[str, Any]:
    pubsub_schema: Schema
    try:
        pubsub_schema = SchemaServiceClient().get_schema({'name': schema_path})
    except NotFound:
        raise NotFound(f'Schema not found: "{schema_path}".')

    return parse_schema(loads(pubsub_schema.definition))


def get_pipeline_options(argv: Union[Sequence[str], None]) -> PipelineOptions:
    pipeline_options: PipelineOptions = PipelineOptions(ArgumentParser().parse_known_args(argv)[1])

    pipeline_options.view_as(SetupOptions).save_main_session = True

    return pipeline_options


def main(argv: Union[Sequence[str], None] = None):
    dataflow_pipeline_options: DataflowPipelineOptions = get_dataflow_pipeline_options()
    dataflow_pipeline_options_map: Dict[str, Any] = dataflow_pipeline_options.get_all_options()
    project: str = dataflow_pipeline_options_map.get('project')
    input_subscription: str = dataflow_pipeline_options_map.get('input_subscription')
    output_table: str = dataflow_pipeline_options_map.get('output_table')
    subscription_path: str = SubscriberClient.subscription_path(project, input_subscription)
    subscription: Subscription = get_subscription(subscription_path)
    topic: Topic = get_topic(subscription.topic)
    encoding: Encoding = topic.schema_settings.encoding
    schema: Dict[str, Any] = get_schema(topic.schema_settings.schema)
    bigquery_schema: Dict[str, List[Dict[str, Any]]] = get_bigquery_schema(schema)
    output_table_id: str = output_table.split('.')[-1]
    output_dataset_id: str = output_table.split('.')[-2]
    pipeline: Pipeline = Pipeline(options=dataflow_pipeline_options)

    logging.info("-----------------------------------------------------------")
    logging.info("          Dataflow AVRO Streaming with Pub/Sub             ")
    logging.info("-----------------------------------------------------------")

    source = ReadFromPubSub(subscription=subscription_path)

    (pipeline
     | "read" >> source
     | "deserialize" >> Map(lambda x: deserialize_avro(x, schema, encoding))
     | "write" >> WriteToBigQuery(table=output_table_id,
                                  dataset=output_dataset_id,
                                  project=project,
                                  schema=bigquery_schema,
                                  create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                                  write_disposition=BigQueryDisposition.WRITE_APPEND))

    pipeline.run()


if __name__ == "__main__":
    main()
