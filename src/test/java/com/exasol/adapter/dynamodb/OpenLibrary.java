package com.exasol.adapter.dynamodb;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;

/**
 * This class imports the open library dataset into a DynamoDB table.
 *
 * run via
 * {@code  mvn  -Dexec.mainClass="com.exasol.adapter.dynamodb.OpenLibrary" -Dexec.classpathScope=test  test-compile exec:java}
 * 
 * for better performance you probably want to run this setup on an ec2 instance. Therefore run the following command in
 * the tools directory:
 * {@code ./runMavenOnAws.sh -Dexec.mainClass="com.exasol.adapter.dynamodb.OpenLibrary" -Dexec.classpathScope=test  test-compile exec:java}
 */
public class OpenLibrary {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenLibrary.class);
    private static final String DATASET_URL = "http://openlibrary.org/data/ol_cdump_latest.txt.gz";
    private static final String LOCAL_DATASET_PATH = "/data/dataset.txt.gz";
    private static final String TABLE_NAME = "open_library_test";
    private final DynamoDB documentClient;

    private OpenLibrary() throws IOException {
        // credentials on ec2 instance are granted by an instance profile created by the terraform script.
        final AmazonDynamoDB lowLevelClient = AmazonDynamoDBClientBuilder.standard().build();
        this.documentClient = new DynamoDB(lowLevelClient);
    }

    public static void main(final String[] args) throws IOException {
        final OpenLibrary openLibrary = new OpenLibrary();
        openLibrary.downloadDatasetIfNotPresent();
        openLibrary.importTestData();
    }

    void importTestData() throws IOException {
        try (final FileInputStream fileInputStream = new FileInputStream(new File(LOCAL_DATASET_PATH));
                final GZIPInputStream unzipedInputStream = new GZIPInputStream(fileInputStream)) {
            importDataFromJsonLines(TABLE_NAME, unzipedInputStream, -1);
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
                LOGGER.error("failed reading wget output");
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
            final BatchWriter batchWriter = new BatchWriter(this.documentClient, tableName, limit);
            try {
                reader.lines().forEach(batchWriter);
            } catch (final LimitExceededException exception) {
                // ignored, as limit was reached.
            }
            batchWriter.flush();
            LOGGER.info("Written items: " + batchWriter.getItemCounter());
            LOGGER.info("Errors: " + batchWriter.getErrorCounter());
        }
    }

    private static class LimitExceededException extends RuntimeException {
        private static final long serialVersionUID = -261552819884874212L;
    }

    private static class BatchWriter implements Consumer<String> {
        private static final int BATCH_SIZE = 20;
        final List<Item> batch = new ArrayList<>(BATCH_SIZE);
        private final DynamoDB dynamoClient;
        private final String tableName;
        private final long itemLimit;
        private long itemCounter;
        private long errorCounter = 0;

        private BatchWriter(final DynamoDB dynamoClient, final String tableName, final long itemLimit) {
            this.dynamoClient = dynamoClient;
            this.tableName = tableName;
            this.itemLimit = itemLimit;
        }

        @Override
        public void accept(final String line) {
            final String[] parts = line.split("\t");
            final String json = String.join("\t", Arrays.asList(parts).subList(4, parts.length));
            this.itemCounter++;
            if (this.itemLimit != -1 && this.itemCounter > this.itemLimit) {
                throw new LimitExceededException();
            }
            final Item item = Item.fromJSON(json);
            this.batch.add(item);
            if (this.batch.size() >= BATCH_SIZE) {
                flush();
            }
        }

        public void flush() {
            if (this.batch.isEmpty()) {
                return;
            }
            final TableWriteItems writeRequest = new TableWriteItems(this.tableName).withItemsToPut(this.batch);
            try {
                final BatchWriteItemOutcome batchWriteItemOutcome = this.dynamoClient.batchWriteItem(writeRequest);
                LOGGER.info("# Writte items: " + this.itemCounter);
            } catch (final AmazonDynamoDBException exception) {
                LOGGER.error(exception.getMessage());
                this.errorCounter++;
            }
            this.batch.clear();
        }

        public long getItemCounter() {
            return this.itemCounter;
        }

        public long getErrorCounter() {
            return this.errorCounter;
        }
    }
}
