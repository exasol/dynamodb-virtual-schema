package com.exasol.adapter.document.dynamodb.connection;

import org.junit.jupiter.api.Test;

import nl.jqno.equalsverifier.EqualsVerifier;

class DynamodbConnectionPropertiesTest {

    @Test
    void testEqualsContract() {
        EqualsVerifier.forClass(DynamodbConnectionProperties.class).verify();
    }
}
