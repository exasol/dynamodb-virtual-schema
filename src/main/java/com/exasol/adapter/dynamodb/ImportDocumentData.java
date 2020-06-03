package com.exasol.adapter.dynamodb;

import com.exasol.ExaIterator;
import com.exasol.ExaMetadata;

/**
 * Main UDF entry point.
 */
public class ImportDocumentData {
    public static final String UDF_NAME = "IMPORT_DOCUMENT_DATA";

    /**
     * This method is called by the Exasol database when the ImportFromDynamodb UDF is called.
     * 
     * @param exaMetadata exasol metadata
     * @param exaIterator iterator
     * @throws Exception if data can't get loaded
     */
    @SuppressWarnings("java:S112") // Exception is too generic. This signature is however given by the UDF framework
    public static void run(final ExaMetadata exaMetadata, final ExaIterator exaIterator) throws Exception {
        // TODO dispatch
        new DynamodbUdf().run(exaMetadata, exaIterator);
    }

    private ImportDocumentData() {
        // Intentionally empty. As this class is only accessed statical.
    }
}
