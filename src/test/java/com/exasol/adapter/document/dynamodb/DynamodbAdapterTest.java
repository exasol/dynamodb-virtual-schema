package com.exasol.adapter.document.dynamodb;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.junit.jupiter.api.Test;

class DynamodbAdapterTest {

    @Test
    void testCapabilitiesAreSupportedByVsCommonDocument() {
        assertDoesNotThrow(() -> new DynamodbAdapter().getCapabilities(null, null));
    }
}