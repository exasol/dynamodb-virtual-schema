package com.exasol.adapter.document;

import java.io.*;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * This class imports the open library dataset into a DynamoDB table.
 *
 * Run via
 * {@code mvn -Dexec.mainClass="com.exasol.adapter.dynamodb.OpenLibrary" -Dexec.classpathScope=test test-compile exec:java}
 * 
 * For better performance you probably want to run this setup on an EC2 instance. Therefore run the following command in
 * the tools directory:
 * {@code ./runMavenOnAws.sh -Dexec.mainClass="com.exasol.adapter.dynamodb.OpenLibrary" -Dexec.classpathScope=test test-compile exec:java}
 */
public class OpenLibrary {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenLibrary.class);
    private static final String DATASET_URL = "http://openlibrary.org/data/ol_cdump_latest.txt.gz";
    private static final String LOCAL_DATASET_PATH = "/data/dataset.txt.gz";
    private static final String TABLE_NAME = "open_library_test";
    private final DynamoDbClient documentClient;

    private OpenLibrary() throws IOException {
        // credentials on ec2 instance are granted by an instance profile created by the terraform script.
        this.documentClient = DynamoDbClient.builder().build();
    }

    public static void main(final String[] args) throws IOException {
        final OpenLibrary openLibrary = new OpenLibrary();
        openLibrary.downloadDatasetIfNotPresent();
        openLibrary.importTestData();
    }

    void importTestData() throws IOException {
        try (final FileInputStream fileInputStream = new FileInputStream(new File(LOCAL_DATASET_PATH));
                final GZIPInputStream unzippedInputStream = new GZIPInputStream(fileInputStream)) {
            importDataFromJsonLines(TABLE_NAME, unzippedInputStream, -1);
        }
    }

    void downloadDatasetIfNotPresent() {
        if (!new File(LOCAL_DATASET_PATH).exists()) {
            final Runtime runtime = Runtime.getRuntime();
            final String[] command = { "wget", DATASET_URL, "-O", LOCAL_DATASET_PATH };
            try {
                final Process wgetProcess = runtime.exec(command);
                final BufferedReader stdErr = new BufferedReader(new InputStreamReader(wgetProcess.getErrorStream()));
                String line;
                while ((line = stdErr.readLine()) != null) {
                    LOGGER.info(line);
                }
                System.out.println("done");
            } catch (final IOException exception) {
                LOGGER.error("failed reading wget output", exception);
            }
        }
    }

    /**
     * Imports data JSON lines files. A JSON lines file has a JSON document in each line.
     *
     * @param tableName   name of the DynamoDB table
     * @param inputStream from the line JSON file to import
     * @param limit       maximum number of items to import;set to {@code -1} to import all items
     * @throws IOException if file can't get opened
     */
    private void importDataFromJsonLines(final String tableName, final InputStream inputStream, final long limit)
            throws IOException {
        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            final DynamodbBatchWriter batchWriter = new DynamodbBatchWriter(this.documentClient, tableName, limit);
            try {
                reader.lines().map(line -> {
                    final String[] parts = line.split("\t");
                    return String.join("\t", Arrays.asList(parts).subList(4, parts.length));
                }).forEach(batchWriter);
            } catch (final DynamodbBatchWriter.LimitExceededException exception) {
                // ignored, as limit was reached.
            }
            batchWriter.flush();
            LOGGER.info("Written items: " + batchWriter.getItemCounter());
            LOGGER.info("Errors: " + batchWriter.getErrorCounter());
        }
    }

}
