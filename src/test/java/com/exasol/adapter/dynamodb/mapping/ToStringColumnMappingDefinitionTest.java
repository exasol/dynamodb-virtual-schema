package com.exasol.adapter.dynamodb.mapping;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.metadata.DataType;

public class ToStringColumnMappingDefinitionTest {
    private static final String DEST_COLUMN = "destColumn";

    @Test
    void testDestinationDataType() {
        final int stringLength = 10;
        final ToStringColumnMappingDefinition toStringColumnMappingDefinition = new ToStringColumnMappingDefinition(
                new AbstractColumnMappingDefinition.ConstructorParameters(DEST_COLUMN,
                        new DocumentPathExpression.Builder().build(),
                        AbstractColumnMappingDefinition.LookupFailBehaviour.DEFAULT_VALUE),
                stringLength, ToStringColumnMappingDefinition.OverflowBehaviour.TRUNCATE);
        assertAll(
                () -> assertThat(toStringColumnMappingDefinition.getExasolDataType().getExaDataType(),
                        equalTo(DataType.ExaDataType.VARCHAR)),
                () -> assertThat(toStringColumnMappingDefinition.getExasolDataType().getSize(), equalTo(stringLength)));
    }

    @Test
    void testGetDestinationDefaultValue() {
        final AbstractColumnMappingDefinition.ConstructorParameters columnParameters = new AbstractColumnMappingDefinition.ConstructorParameters(
                null, null, null);
        final ToStringColumnMappingDefinition toStringColumnMappingDefinition = new ToStringColumnMappingDefinition(
                columnParameters, 0, null);
        assertThat(toStringColumnMappingDefinition.getExasolDefaultValue().toString(), equalTo(""));
    }

    @Test
    void testIsDestinationNullable() {
        final AbstractColumnMappingDefinition.ConstructorParameters columnParameters = new AbstractColumnMappingDefinition.ConstructorParameters(
                null, null, null);
        final ToStringColumnMappingDefinition toStringColumnMappingDefinition = new ToStringColumnMappingDefinition(
                columnParameters, 0, null);
        assertThat(toStringColumnMappingDefinition.isExasolColumnNullable(), equalTo(true));
    }
}
