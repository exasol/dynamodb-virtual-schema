package com.exasol.adapter.document.dynamodb;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.document.DocumentAdapter;

class DynamodbDocumentAdapterDialectTest {

    @Test
    void testCapabilitiesAreSupportedByVsCommonDocument() {
        assertDoesNotThrow(() -> new DocumentAdapter(new DynamodbDocumentAdapterDialect()).getCapabilities(null, null));
    }
}