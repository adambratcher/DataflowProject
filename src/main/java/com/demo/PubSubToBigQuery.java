package com.demo;

import com.demo.SchemaUtils;

import org.apache.avro.Schema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.values.Row;


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

        @Description("Is PubSub message JSON format")
        @Default.Boolean(false)
        boolean getJsonFormat();

        void setJsonFormat(boolean jsonFormat);

        @Description("GCS path to Avro schema file")
        @Validation.Required
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
        PCollection<PubsubMessage> messages = null;
        if (options.getUseSubscription()) {
            messages =
                    pipeline.apply(
                            "Read PubSub Subscription",
                            PubsubIO.readMessagesWithAttributes()
                                    .fromSubscription(options.getInputSubscription())
                    );
        } else {
            messages =
                    pipeline.apply(
                            "Read PubSub Topic",
                            PubsubIO.readMessagesWithAttributes()
                                    .fromTopic(options.getInputTopic())
                    );
        }

        /*
         * Step #2: Write Records to BigQuery
         */
        WriteResult writeResult = (WriteResult) messages
                .apply("Convert To Tablerows", Convert.toRows())
                .apply(
                "Write to BigQuery", BigQueryIO.writeTableRows()
                                .useBeamSchema()
                                .useAvroLogicalTypes());

        return pipeline.run();
    }
}
