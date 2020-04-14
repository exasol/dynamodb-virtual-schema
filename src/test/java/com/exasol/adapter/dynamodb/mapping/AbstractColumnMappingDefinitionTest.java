package com.exasol.adapter.dynamodb.mapping;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.jupiter.api.Test;

public class AbstractColumnMappingDefinitionTest {
    private static final String EXASOL_COLUMN_NAME = "columnName";

    @Test
    void testColumnName() {
        final MockColumnMappingDefinition columnMappingDefinition = new MockColumnMappingDefinition(EXASOL_COLUMN_NAME,
                null, null);
        assertThat(columnMappingDefinition.getExasolColumnName(), equalTo(EXASOL_COLUMN_NAME));
    }

    @Test
    void testGetDestinationDefaultValueLiteral() {
        final MockColumnMappingDefinition columnMappingDefinition = new MockColumnMappingDefinition(EXASOL_COLUMN_NAME,
                null, null);
        assertThat(columnMappingDefinition.getExasolDefaultValueLiteral(), equalTo("'default'"));
    }
}
