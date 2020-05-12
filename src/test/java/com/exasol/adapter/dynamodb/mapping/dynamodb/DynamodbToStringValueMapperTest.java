package com.exasol.adapter.dynamodb.mapping.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbList;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNumber;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbString;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.dynamodb.mapping.AbstractPropertyToColumnMapping;
import com.exasol.adapter.dynamodb.mapping.LookupFailBehaviour;
import com.exasol.adapter.dynamodb.mapping.ToStringPropertyToColumnMapping;

public class DynamodbToStringValueMapperTest {

    private static final int TEST_NUMBER = 42;
    private static final String TEST_SOURCE_COLUMN = "myColumn";
    private static final String DEST_COLUMN = "destColumn";

    private static final DocumentPathExpression TEST_SOURCE_COLUMN_PATH = new DocumentPathExpression.Builder()
            .addObjectLookup(TEST_SOURCE_COLUMN).build();
    private static final AbstractPropertyToColumnMapping.ConstructorParameters COLUMN_PARAMETERS = new AbstractPropertyToColumnMapping.ConstructorParameters(
            DEST_COLUMN, TEST_SOURCE_COLUMN_PATH, LookupFailBehaviour.DEFAULT_VALUE);

    @Test
    void testConvertStringRow() {
        final String testString = "test";
        final ToStringPropertyToColumnMapping toStringColumnMappingDefinition = new ToStringPropertyToColumnMapping(
                COLUMN_PARAMETERS, 100, null);
        final String result = new DynamodbToStringValueMapper(toStringColumnMappingDefinition)
                .mapStringValue(new DynamodbString(testString));
        assertThat(result, equalTo(testString));
    }

    @Test
    void testConvertNumberRow() {
        final ToStringPropertyToColumnMapping toStringColumnMappingDefinition = new ToStringPropertyToColumnMapping(
                COLUMN_PARAMETERS, 100, null);
        final String result = new DynamodbToStringValueMapper(toStringColumnMappingDefinition)
                .mapStringValue(new DynamodbNumber(String.valueOf(TEST_NUMBER)));
        assertThat(result, equalTo(String.valueOf(TEST_NUMBER)));
    }

    @Test
    void testConvertUnsupportedDynamodbType() {
        final ToStringPropertyToColumnMapping toStringColumnMappingDefinition = new ToStringPropertyToColumnMapping(
                COLUMN_PARAMETERS, 100, null);
        final UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class,
                () -> new DynamodbToStringValueMapper(toStringColumnMappingDefinition)
                        .mapStringValue(new DynamodbList(Collections.emptyList())));
        assertThat(exception.getMessage(),
                equalTo("The DynamoDB type List cant't be converted to string. Try using a different mapping."));
    }
}
