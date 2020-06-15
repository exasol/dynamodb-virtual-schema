package com.exasol.adapter.dynamodb;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;

/**
 * This class imports the open library dataset into a DynamoDB table.
 */
public class OpenLibrary {
    private static final String DATASET_URL = "https://s3-eu-west-1.amazonaws.com/csparkdata/ol_cdump.json";
    private static final String TMP_FILE_NAME = "/tmp/dataset.json";
    private static final String TABLE_NAME = "open_library_test";
    private final DynamodbTestInterface dynamodbTestInterface;

    public OpenLibrary(final DynamodbTestInterface dynamodbTestInterface) throws IOException {
        this.dynamodbTestInterface = dynamodbTestInterface;
        if (dynamodbTestInterface.isTableEmpty(TABLE_NAME)) {
            loadTestData();
            importTestData();
        }
    }

    void loadTestData() throws IOException {
        final URL url = new URL(DATASET_URL);
        final ReadableByteChannel readableByteChannel = Channels.newChannel(url.openStream());
        final FileOutputStream fileOutputStream = new FileOutputStream(TMP_FILE_NAME);
        final FileChannel fileChannel = fileOutputStream.getChannel();
        fileOutputStream.getChannel().transferFrom(readableByteChannel, 0, Long.MAX_VALUE);
    }

    void importTestData() throws IOException {
        this.dynamodbTestInterface.importDataFromJsonLines(TABLE_NAME, new File(TMP_FILE_NAME));
    }
}