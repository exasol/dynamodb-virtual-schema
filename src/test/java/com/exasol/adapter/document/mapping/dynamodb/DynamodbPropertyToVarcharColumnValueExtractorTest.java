package com.exasol.adapter.document.mapping.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.document.documentnode.dynamodb.DynamodbList;
import com.exasol.adapter.document.documentnode.dynamodb.DynamodbNumber;
import com.exasol.adapter.document.documentnode.dynamodb.DynamodbString;
import com.exasol.adapter.document.documentpath.DocumentPathExpression;
import com.exasol.adapter.document.mapping.MappingErrorBehaviour;
import com.exasol.adapter.document.mapping.PropertyToVarcharColumnMapping;
import com.exasol.adapter.document.mapping.TruncateableMappingErrorBehaviour;

class DynamodbPropertyToVarcharColumnValueExtractorTest {

    private static final int TEST_NUMBER = 42;
    private static final String TEST_SOURCE_COLUMN = "myColumn";
    private static final String DEST_COLUMN = "destColumn";

    private static final DocumentPathExpression TEST_SOURCE_COLUMN_PATH = DocumentPathExpression.builder()
            .addObjectLookup(TEST_SOURCE_COLUMN).build();

    private static final PropertyToVarcharColumnMapping TO_STRING_PROPERTY_TO_COLUMN_MAPPING = PropertyToVarcharColumnMapping
            .builder()//
            .exasolColumnName(DEST_COLUMN)//
            .pathToSourceProperty(TEST_SOURCE_COLUMN_PATH)//
            .lookupFailBehaviour(MappingErrorBehaviour.NULL)//
            .varcharColumnSize(100)//
            .overflowBehaviour(TruncateableMappingErrorBehaviour.ABORT)//
            .build();

    @Test
    void testConvertStringRow() {
        final String testString = "test";
        final String result = new DynamodbPropertyToVarcharColumnValueExtractor(TO_STRING_PROPERTY_TO_COLUMN_MAPPING)
                .mapStringValue(new DynamodbString(testString));
        assertThat(result, equalTo(testString));
    }

    @Test
    void testConvertNumberRow() {
        final String result = new DynamodbPropertyToVarcharColumnValueExtractor(TO_STRING_PROPERTY_TO_COLUMN_MAPPING)
                .mapStringValue(new DynamodbNumber(String.valueOf(TEST_NUMBER)));
        assertThat(result, equalTo(String.valueOf(TEST_NUMBER)));
    }

    @Test
    void testConvertUnsupportedDynamodbType() {
        final DynamodbPropertyToVarcharColumnValueExtractor valueExtractor = new DynamodbPropertyToVarcharColumnValueExtractor(
                TO_STRING_PROPERTY_TO_COLUMN_MAPPING);
        final DynamodbList dynamodbList = new DynamodbList(Collections.emptyList());
        final UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class,
                () -> valueExtractor.mapStringValue(dynamodbList));
        assertThat(exception.getMessage(),
                equalTo("The DynamoDB type List cant't be converted to string. Try using a different mapping."));
    }
}
