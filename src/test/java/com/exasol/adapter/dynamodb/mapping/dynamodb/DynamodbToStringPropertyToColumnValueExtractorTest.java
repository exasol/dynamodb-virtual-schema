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
import com.exasol.adapter.dynamodb.mapping.LookupFailBehaviour;
import com.exasol.adapter.dynamodb.mapping.ToStringPropertyToColumnMapping;

class DynamodbToStringPropertyToColumnValueExtractorTest {

    private static final int TEST_NUMBER = 42;
    private static final String TEST_SOURCE_COLUMN = "myColumn";
    private static final String DEST_COLUMN = "destColumn";

    private static final DocumentPathExpression TEST_SOURCE_COLUMN_PATH = new DocumentPathExpression.Builder()
            .addObjectLookup(TEST_SOURCE_COLUMN).build();

    private static final ToStringPropertyToColumnMapping TO_STRING_PROPERTY_TO_COLUMN_MAPPING = new ToStringPropertyToColumnMapping(
            DEST_COLUMN, TEST_SOURCE_COLUMN_PATH, LookupFailBehaviour.DEFAULT_VALUE, 100, null);

    @Test
    void testConvertStringRow() {
        final String testString = "test";
        final String result = new DynamodbToStringPropertyToColumnValueExtractor(TO_STRING_PROPERTY_TO_COLUMN_MAPPING)
                .mapStringValue(new DynamodbString(testString));
        assertThat(result, equalTo(testString));
    }

    @Test
    void testConvertNumberRow() {
        final String result = new DynamodbToStringPropertyToColumnValueExtractor(TO_STRING_PROPERTY_TO_COLUMN_MAPPING)
                .mapStringValue(new DynamodbNumber(String.valueOf(TEST_NUMBER)));
        assertThat(result, equalTo(String.valueOf(TEST_NUMBER)));
    }

    @Test
    void testConvertUnsupportedDynamodbType() {
        final DynamodbToStringPropertyToColumnValueExtractor valueExtractor = new DynamodbToStringPropertyToColumnValueExtractor(
                TO_STRING_PROPERTY_TO_COLUMN_MAPPING);
        final DynamodbList dynamodbList = new DynamodbList(Collections.emptyList());
        final UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class,
                () -> valueExtractor.mapStringValue(dynamodbList));
        assertThat(exception.getMessage(),
                equalTo("The DynamoDB type List cant't be converted to string. Try using a different mapping."));
    }
}
