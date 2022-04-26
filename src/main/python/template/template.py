import json
import logging
from argparse import ArgumentParser
from datetime import datetime
from io import StringIO
from typing import Any, Dict, List, Tuple, Union

import json5
from apache_beam import Map, Pipeline
from apache_beam.io import BigQueryDisposition, ReadFromPubSub, WriteToBigQuery, WriteToPubSub
from apache_beam.io.gcp.bigquery_tools import RetryStrategy
from apache_beam.options.pipeline_options import PipelineOptions
from fastavro import json_reader, json_writer, parse_schema, reader
from google.api_core.exceptions import NotFound
from google.pubsub_v1 import Encoding, GetSubscriptionRequest, PublisherClient, Schema, SchemaServiceClient, \
    SubscriberClient, Subscription, Topic

logging.basicConfig(format="%(asctime)s : %(levelname)s : %(name)s : %(message)s", level=logging.INFO)
logging = logging.getLogger(__name__)


class SchemaConversionError(Exception):
    pass


class DataflowPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser: ArgumentParser):
        parser.add_argument("--input_subscription", required=True, help="pubsub input topic")
        parser.add_argument("--output_table", required=True, help="bigquery output table")
        parser.add_argument("--dead_letter_topic", required=False, help="pubsub dead_letter topic")


class PubSubPipeline:

    def __init__(self, project_id: str, subscription_id: Union[str, None] = None, topic_id: Union[str, None] = None,
                 schema_id: Union[str, None] = None):
        self.subscription: Union[Subscription, None] = self.__get_subscription(project_id, subscription_id)
        self.topic: Union[Topic, None] = self.__get_topic(project_id, topic_id, self.subscription)
        self.schema: Union[Schema, None] = self.__get_schema(project_id, schema_id, self.topic)

    @classmethod
    def __get_schema(cls, project_id: str, schema_id: Union[str, None] = None,
                     topic: Union[Topic, None] = None) -> Union[Schema, None]:
        schema_path: Union[str, None] = cls.__get_schema_path(project_id, schema_id, topic)

        if not schema_path:
            return None

        try:
            return SchemaServiceClient().get_schema({"name": schema_path})
        except NotFound:
            raise NotFound(f'Schema not found: "{schema_path}".')

    @staticmethod
    def __get_schema_path(project_id: str, schema_id: Union[str, None] = None,
                          topic: Union[Topic, None] = None) -> Union[str, None]:
        if schema_id:
            return SchemaServiceClient.schema_path(project_id, schema_id)

        if topic:
            return topic.schema_settings.schema

        return None

    @classmethod
    def __get_subscription(cls, project_id: str, subscription_id: Union[str, None] = None) -> Union[Subscription, None]:
        subscription_path: Union[str, None] = cls.__get_subscription_path(project_id, subscription_id)

        if not subscription_path:
            return None

        try:
            return SubscriberClient().get_subscription(
                request=GetSubscriptionRequest({"subscription": subscription_path}))
        except NotFound:
            raise NotFound(f'Subscription not found: "{subscription_path}".')

    @staticmethod
    def __get_subscription_path(project_id: str, subscription_id: Union[str, None] = None) -> Union[str, None]:
        if subscription_id:
            return SubscriberClient.subscription_path(project_id, subscription_id)

        return None

    @classmethod
    def __get_topic(cls, project_id: str, topic_id: Union[str, None] = None,
                    subscription: Union[Subscription, None] = None) -> Union[Topic, None]:
        topic_path: Union[str, None] = cls.__get_topic_path(project_id, topic_id, subscription)

        if not topic_path:
            return None

        try:
            return PublisherClient().get_topic(request={"topic": topic_path})
        except NotFound:
            raise NotFound(f'Topic not found: "{topic_path}".')

    @staticmethod
    def __get_topic_path(project_id: str, topic_id: Union[str, None] = None,
                         subscription: Union[Subscription, None] = None) -> Union[str, None]:
        if topic_id:
            return PublisherClient.topic_path(project_id, topic_id)

        if subscription:
            return subscription.topic

        return None


class AvroService:

    def __init__(self, schema: Schema, encoding: Encoding):
        self.schema: Dict[str, Any] = self.__get_parsed_schema(schema)
        self.encoding: Encoding = encoding

    def deserialize(self, record: [str, bytes]) -> Dict[str, Any]:
        if self.encoding is Encoding.JSON:
            return self.__deserialize_json(record)

        if self.encoding is Encoding.BINARY:
            raise NotImplementedError("Avro binary deserialization has not been implemented yet.")

        raise TypeError("No deserialization method is available for this type of encoding.", self.encoding)

    def serialize(self, record: Dict[str, Any]) -> [str, bytes]:
        if self.encoding is Encoding.JSON:
            return self.__serialize_json(record)

        if self.encoding is Encoding.BINARY:
            raise NotImplementedError("Avro binary serialization has not been implemented yet.")

        raise TypeError("No serialization method is available for this type of encoding.", self.encoding)

    def __deserialize_json(self, record: str) -> Dict[str, Any]:
        string_reader: StringIO = StringIO(self.__strip_json_whitespaces(record))

        avro_reader: reader = json_reader(string_reader, self.schema)

        return avro_reader.next()

    def __serialize_json(self, record: Dict[str, Any]) -> str:
        string_writer: StringIO = StringIO()

        json_writer(string_writer, self.schema, [record])

        return string_writer.getvalue()

    @staticmethod
    def __get_parsed_schema(schema: Schema) -> Dict[str, Any]:
        return parse_schema(json.loads(schema.definition))

    @staticmethod
    def __strip_json_whitespaces(json_string: str) -> str:
        return json.dumps(json5.loads(json_string), separators=(',', ':'))


class AvroToBigQuerySchemaConverter:
    avro_type_to_bigquery_type_map: Dict[Tuple[str, Union[str, None]], str] = {
        ("boolean", None): "BOOLEAN",
        ("bytes", None): "BYTES",
        ("double", None): "FLOAT",
        ("enum", None): "STRING",
        ("float", None): "FLOAT",
        ("int", "date"): "DATE",
        ("int", "time-millis"): "TIME",
        ("int", None): "INTEGER",
        ("long", "time-micros"): "TIME",
        ("long", "timestamp-micros"): "TIMESTAMP",
        ("long", "timestamp-millis"): "TIMESTAMP",
        ("long", None): "INTEGER",
        ("record", None): "RECORD",
        ("string", "uuid"): "STRING",
        ("string", None): "STRING",
    }

    def __init__(self):
        self.__schema_conversion_errors: List[Tuple[str, Any]] = []

    def convert(self, avro_schema: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        bigquery_record_field_definitions: List[Dict[str, Any]] = \
            self.__get_record_field_definitions(avro_schema)

        if self.__schema_conversion_errors:
            raise SchemaConversionError('Errors found in Avro to BigQuery schema conversion.',
                                        self.__schema_conversion_errors)

        return {"fields": bigquery_record_field_definitions}

    def __find_next_avro_record_fields(self, avro_schema: Dict[str, Any]) -> List[Dict[str, Any]]:
        if "fields" in avro_schema:
            return avro_schema["fields"]

        if "type" in avro_schema:
            if isinstance(avro_schema["type"], dict):
                return self.__find_next_avro_record_fields(avro_schema["type"])

            if isinstance(avro_schema["type"], list):
                return self.__find_next_avro_record_fields(
                    [field_type for field_type in avro_schema["type"] if field_type != "null"][0])

        if "items" in avro_schema:
            return self.__find_next_avro_record_fields(avro_schema["items"])

        return []

    def __get_field_definition(self, field: Dict[str, Any]) -> Union[Dict[str, Any], None]:
        bigquery_field_definition: Dict[str, Any] = {}

        if "name" not in field:
            return self.__schema_conversion_errors.append(
                ('The "name" key was not found in the Avro field definition.', field))

        if "type" not in field:
            return self.__schema_conversion_errors.append(
                ('The "type" key was not found in the Avro field definition.', field))

        bigquery_field_definition["name"] = field["name"]
        bigquery_field_definition["type"] = self.__get_field_type(field["type"])
        bigquery_field_definition["mode"] = self.__get_field_mode(field)

        if bigquery_field_definition["type"] == "RECORD":
            bigquery_field_definition["fields"] = self.__get_record_field_definitions(field)

        return bigquery_field_definition

    @staticmethod
    def __get_field_mode(field: Dict[str, Any]) -> str:
        if "null" in field["type"]:
            return "NULLABLE"

        if "type" in field["type"] and field["type"]["type"] == "array":
            return "REPEATED"

        return "REQUIRED"

    def __get_field_nested_type(self, nested_field_type: Dict[str, Any]) -> Union[str, None]:
        if "type" not in nested_field_type:
            return self.__schema_conversion_errors.append(('The "type" key was not found in nested type field.',
                                                           nested_field_type))

        if nested_field_type["type"] == "array":
            if "items" not in nested_field_type:
                return self.__schema_conversion_errors.append(('The "items" key was not found in array type field.',
                                                               nested_field_type))

            return self.__get_field_type(nested_field_type["items"])

        return self.__get_field_type(nested_field_type["type"])

    def __get_field_type(
            self, field_type: Union[str, Dict[str, Any], List[Union[str, Dict[str, Any]]]]) -> Union[str, None]:
        if isinstance(field_type, list):
            return self.__get_union_field_type(field_type)

        if isinstance(field_type, dict):
            type_value: Any = field_type.get("type")
            logical_type_value: Any = field_type.get("logicalType")

            if isinstance(type_value, (str, type(None))) and isinstance(logical_type_value, (str, type(None))) \
                    and (type_value, logical_type_value) in self.avro_type_to_bigquery_type_map:
                return self.avro_type_to_bigquery_type_map[(type_value, logical_type_value)]

            return self.__get_field_nested_type(field_type)

        if isinstance(field_type, str) and (field_type, None) in self.avro_type_to_bigquery_type_map:
            return self.avro_type_to_bigquery_type_map[(field_type, None)]

        return self.__schema_conversion_errors.append(
            ("A BigQuery type could not be resolved for this field.", field_type))

    def __get_record_field_definitions(self, avro_schema: Dict[str, Any]) -> Union[List[Dict[str, Any]], None]:
        next_avro_record_fields: List[Dict[str, Any]] = self.__find_next_avro_record_fields(avro_schema)

        if not next_avro_record_fields:
            return self.__schema_conversion_errors.append(('Avro record fields could not be found.', avro_schema))

        return [self.__get_field_definition(field) for field in next_avro_record_fields]

    def __get_union_field_type(self, union_field_type: List[Union[str, Dict[str, Any]]]) -> Union[str, None]:
        non_null_union_field_types: List[Union[str, Dict[str, Any]]] = [field_type for field_type in union_field_type
                                                                        if field_type != "null"]

        if len(non_null_union_field_types) > 1:
            return self.__schema_conversion_errors.append(
                ('Union types are not supported for Avro to BigQuery schema conversion.', union_field_type))

        return self.__get_field_type(non_null_union_field_types[0])


def deserialize_record(record: bytes, avro_service: AvroService) -> Dict[str, Any]:
    return avro_service.deserialize(record.decode('utf-8'))


def get_dataflow_pipeline_options() -> DataflowPipelineOptions:
    parser: ArgumentParser = ArgumentParser()
    pipeline_options: PipelineOptions = PipelineOptions(parser.parse_known_args()[1], streaming=True,
                                                        save_main_session=True)

    return pipeline_options.view_as(DataflowPipelineOptions)


def get_dead_letter_avro_service(dead_letter_pub_sub_pipeline: PubSubPipeline,
                                 has_dead_letter_topic: bool) -> Union[AvroService, None]:
    if has_dead_letter_topic:
        return AvroService(dead_letter_pub_sub_pipeline.schema,
                           dead_letter_pub_sub_pipeline.topic.schema_settings.encoding)

    return None


def get_insert_retry_strategy(has_dead_letter_topic: bool) -> RetryStrategy:
    if has_dead_letter_topic:
        return RetryStrategy.RETRY_ON_TRANSIENT_ERROR

    return RetryStrategy.RETRY_ALWAYS


def is_valid_dead_letter_topic(topic: Topic, dead_letter_topic: Topic) -> bool:
    if dead_letter_topic and topic.name != dead_letter_topic.name:
        return True

    return False


def serialize_failed_record(record: Tuple[str, Dict[str, Any]], avro_service: AvroService,
                            dead_letter_avro_service: AvroService, dataflow_job_name: str) -> bytes:
    return dead_letter_avro_service.serialize(
        {'dataflow_job_name': dataflow_job_name,
         'error_datetime': datetime.now(),
         'error_message': "Write To BigQuery Failure",
         'message': avro_service.serialize(record[1])}
    ).encode('utf-8')


def main():
    dataflow_pipeline_options: DataflowPipelineOptions = get_dataflow_pipeline_options()
    dataflow_pipeline_options_map: Dict[str, Any] = dataflow_pipeline_options.get_all_options()
    project: str = dataflow_pipeline_options_map.get('project')
    dataflow_job_name: str = dataflow_pipeline_options_map.get('job_name')
    input_subscription: str = dataflow_pipeline_options_map.get('input_subscription')
    output_table: str = dataflow_pipeline_options_map.get('output_table')
    dead_letter_topic_id: str = dataflow_pipeline_options_map.get('dead_letter_topic')
    pub_sub_pipeline: PubSubPipeline = PubSubPipeline(project, input_subscription)
    avro_service: AvroService = AvroService(pub_sub_pipeline.schema, pub_sub_pipeline.topic.schema_settings.encoding)
    dead_letter_pub_sub_pipeline: PubSubPipeline = PubSubPipeline(project, topic_id=dead_letter_topic_id)
    has_dead_letter_topic: bool = is_valid_dead_letter_topic(pub_sub_pipeline.topic, dead_letter_pub_sub_pipeline.topic)
    dead_letter_avro_service: Union[AvroService, None] = get_dead_letter_avro_service(
        dead_letter_pub_sub_pipeline, has_dead_letter_topic)
    bigquery_schema: Dict[str, List[Dict[str, Any]]] = AvroToBigQuerySchemaConverter().convert(avro_service.schema)
    insert_retry_strategy: RetryStrategy = get_insert_retry_strategy(has_dead_letter_topic)

    logging.info('----------------------------------------------------------')
    logging.info('            Pub/Sub AVRO to BigQuery Streaming            ')
    logging.info('----------------------------------------------------------')

    with Pipeline(options=dataflow_pipeline_options) as pipeline:
        pipeline |= 'PubSub Read' >> ReadFromPubSub(subscription=pub_sub_pipeline.subscription.name)
        pipeline |= 'Records Deserialize' >> Map(lambda record: deserialize_record(record, avro_service))
        pipeline |= 'BigQuery Write' >> WriteToBigQuery(table=output_table,
                                                        project=project,
                                                        schema=bigquery_schema,
                                                        create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                                                        write_disposition=BigQueryDisposition.WRITE_APPEND,
                                                        insert_retry_strategy=insert_retry_strategy)

        if has_dead_letter_topic:
            (pipeline['FailedRows']
             | 'Failed Records Serialize' >> Map(lambda record: serialize_failed_record(
                        record, avro_service, dead_letter_avro_service, dataflow_job_name))
             | 'PubSub Dead Letter Write' >> WriteToPubSub(topic=dead_letter_pub_sub_pipeline.topic.name))


if __name__ == "__main__":
    main()
