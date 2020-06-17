package com.exasol.adapter.dynamodb;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class imports the open library dataset into a DynamoDB table.
 */
public class OpenLibrary {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenLibrary.class);
    private static final String DATASET_URL = "http://openlibrary.org/data/ol_cdump_latest.txt.gz";
    private static final String TMP_FILE_NAME = "/tmp/dataset.txt.gz";
    private static final String TABLE_NAME = "open_library_test";
    private final DynamodbTestInterface dynamodbTestInterface;

    private OpenLibrary() throws IOException {
        this.dynamodbTestInterface = new DynamodbTestInterface();
    }

    public static void main(final String[] args) throws IOException {
        final OpenLibrary openLibrary = new OpenLibrary();
        openLibrary.downloadDatasetIfNotPresent();
    }

    void importTestData() throws IOException {
        this.dynamodbTestInterface.importDataFromJsonLines(TABLE_NAME, new File(TMP_FILE_NAME));
    }

    void downloadDatasetIfNotPresent() throws IOException {
        if (!new File(TMP_FILE_NAME).exists()) {
            final Runtime runtime = Runtime.getRuntime();
            final String[] command = { "wget", DATASET_URL, "-O", TMP_FILE_NAME };
            try {
                final Process wgetProcess = runtime.exec(command);
                final BufferedReader stdOut = new BufferedReader(new InputStreamReader(wgetProcess.getInputStream()));
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
}
