package com.demo;

import com.google.common.io.CharStreams;
import org.apache.avro.Schema;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

public class SchemaUtils {
    /* Logger for class */
    private static final Logger LOG = LoggerFactory.getLogger(SchemaUtils.class);

    /**
     * Reads a file from GCS and returns it as a string.
     *
     * @param filePath path to file in GCS
     * @return contents of the file as a string
     * @throws RuntimeException thrown if not able to read or parse file
     */
    public static String getGcsFileAsString(String filePath) {
        ReadableByteChannel channel = getGcsFileByteChannel(filePath);
        try (Reader reader = Channels.newReader(channel, UTF_8.name())) {
            return CharStreams.toString(reader);
        } catch (IOException e) {
            LOG.error("Error parsing file contents into string: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * Handles getting the {@link ReadableByteChannel} for {@code filePath}.
     */
    private static ReadableByteChannel getGcsFileByteChannel(String filePath) {
        try {
            MatchResult result = FileSystems.match(filePath);
            checkArgument(
                    result.status() == MatchResult.Status.OK && !result.metadata().isEmpty(),
                    "Failed to match any files with the pattern: " + filePath
            );

            List<ResourceId> rId =
                    result.metadata().stream().map(MatchResult.Metadata::resourceId).collect(toList());

            checkArgument(rId.size() == 1, "Expected exactly 1 file, but got " + rId.size() + " files");

            return FileSystems.open(rId.get(0));
        } catch (IOException ioe) {
            LOG.error("File system i/o error: " + ioe.getMessage());
            throw new RuntimeException(ioe);
        }
    }

    /**
     * Reads an Avro schema file from GCS, parses it and
     * returns a new Schema object.
     *
     * @param schemaLocation Path to schema (e.g. gs://mybucket/path/).
     * @return {@link Schema}
     */
    public static Schema getAvroSchema(String schemaLocation) {
        String schemaFile = getGcsFileAsString(schemaLocation);
        return new Schema.Parser().parse(schemaFile);
    }
}
