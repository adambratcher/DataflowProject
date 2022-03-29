package com.demo;

import com.demo.SchemaUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;


public class PubSubToBigQuery {

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(Options.class);

        run(options);
    }

    public interface Options extends PipelineOptions {
        @Description("Table spec to write the output to")
        ValueProvider<String> getOutputTableSpec();

        void setOutputTableSpec(ValueProvider<String> value);

        //FIXME: Do we want to even include read from Topic? Maybe make optional and subscription default?
        @Description("Pub/Sub topic to read the input from")
        ValueProvider<String> getInputTopic();

        void setInputTopic(ValueProvider<String> value);

        @Description(
                "The Cloud Pub/Sub subscription to consume from. "
                        + "The name should be in the format of "
                        + "projects/<project-id>/subscriptions/<subscription-name>.")
        ValueProvider<String> getInputSubscription();

        void setInputSubscription(ValueProvider<String> value);

        @Description("This determines whether the template reads from a pub/sub subscription or a topic")
        @Default.Boolean(false)
        Boolean getUseSubscription();

        void setUseSubscription(Boolean value);

        @Description("GCS path to Avro schema file")
//        @Validation.Required
        String getSchemaPath();

        void setSchemaPath(String schemaPath);
    }

    public static PipelineResult run(Options options) {
        Pipeline pipeline = Pipeline.create(options);

        Schema schema = SchemaUtils.getAvroSchema(options.getSchemaPath());

        /*
         * Step #1: Read messages in from Pub/Sub
         * Either from a Subscription or Topic
         */
        PCollection<String> messages = null;
        if (options.getUseSubscription()) {
            messages =
                    pipeline.apply(
                            "Read PubSub Subscription",
                            PubsubIO.readStrings()
                                    .fromSubscription(options.getInputSubscription())
                    );
        } else {
            messages =
                    pipeline.apply(
                            "Read PubSub Topic",
                            PubsubIO.readStrings()
                                    .fromTopic(options.getInputTopic())
                    );
        }

        /*
         * Step #2: Transform JSON -> Avro
         * The purpose of this is to load Avro into BigQuery and
         * BigQuery Schema can be inferred from Avro
         */
        PCollection<GenericRecord> avroRecords = messages.apply("Make Generic Record",
                MapElements.via(JsonToAvroFn.of(schema)))
                .setCoder(AvroUtils.schemaCoder(schema));

        /*
        * Step #3: Write to BigQuery
        * BigQuery table is created if it does not already exist using
        * Avro Schema and using Avro Logical Types
         */
        avroRecords.apply(
                "Write Avro to BigQuery",
                BigQueryIO.<GenericRecord>write()
                        .useBeamSchema()
                        .useAvroLogicalTypes()
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .to(options.getOutputTableSpec())
        );

        return pipeline.run();
    }

    /*
    * Transform JSON Objects to GenericRecord
     */
    public static class JsonToAvroFn extends SimpleFunction<String, GenericRecord> {

        private final String schema;

        public JsonToAvroFn(String schema) {
            this.schema = schema;
        }

        public static JsonToAvroFn of(String avroSchema) {
            return new JsonToAvroFn(avroSchema);
        }

        public static JsonToAvroFn of(Schema avroSchema) {
            return of(avroSchema.toString());
        }

        @Override
        public GenericRecord apply(String avroJson) {
            try {
                Schema avroSchema = getSchema();
                return new GenericDatumReader<GenericRecord>(avroSchema)
                        .read(null, DecoderFactory.get().jsonDecoder(avroSchema, avroJson));
            } catch (IOException ioe) {
                throw new RuntimeException("Error parsing Avro JSON", ioe);
            }
        }

        private Schema getSchema() {
            return new Schema.Parser().parse(schema);
        }
    }
}
