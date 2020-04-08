package com.exasol.adapter.dynamodb.mapping;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.jupiter.api.Test;

public class AbstractColumnMappingDefinitionTest {
    @Test
    void testDestinationName() {
        final String destinationName = "destinationName";
        final MocColumnMappingDefinition columnMappingDefinition = new MocColumnMappingDefinition(destinationName, null,
                null);
        assertThat(columnMappingDefinition.getExasolColumnName(), equalTo(destinationName));
    }

    @Test
    void testGetDestinationDefaultValueLiteral() {
        final String destinationName = "destinationName";
        final MocColumnMappingDefinition columnMappingDefinition = new MocColumnMappingDefinition(destinationName, null,
                null);
        assertThat(columnMappingDefinition.getExasolDefaultValueLiteral(), equalTo("'default'"));
    }
}
