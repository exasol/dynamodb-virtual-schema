package com.exasol.adapter.document.mapping.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.util.Collections;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.document.documentnode.dynamodb.*;
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
    void testConvertString() {
        final String testString = "test";
        final var result = new DynamodbPropertyToVarcharColumnValueExtractor(TO_STRING_PROPERTY_TO_COLUMN_MAPPING)
                .mapStringValue(new DynamodbString(testString));
        assertAll(//
                () -> assertThat(result.isConverted(), equalTo(false)),
                () -> assertThat(result.getValue(), equalTo(testString))//
        );
    }

    @Test
    void testConvertNumber() {
        final var result = new DynamodbPropertyToVarcharColumnValueExtractor(TO_STRING_PROPERTY_TO_COLUMN_MAPPING)
                .mapStringValue(new DynamodbNumber(String.valueOf(TEST_NUMBER)));
        assertAll(//
                () -> assertThat(result.isConverted(), equalTo(true)),
                () -> assertThat(result.getValue(), equalTo(String.valueOf(TEST_NUMBER)))//
        );
    }

    @Test
    void testConvertTrue() {
        final var result = new DynamodbPropertyToVarcharColumnValueExtractor(TO_STRING_PROPERTY_TO_COLUMN_MAPPING)
                .mapStringValue(new DynamodbBoolean(true));
        assertAll(//
                () -> assertThat(result.isConverted(), equalTo(true)),
                () -> assertThat(result.getValue(), equalTo("true"))//
        );
    }

    @Test
    void testConvertFalse() {
        final var result = new DynamodbPropertyToVarcharColumnValueExtractor(TO_STRING_PROPERTY_TO_COLUMN_MAPPING)
                .mapStringValue(new DynamodbBoolean(false));
        assertAll(//
                () -> assertThat(result.isConverted(), equalTo(true)),
                () -> assertThat(result.getValue(), equalTo("false"))//
        );
    }

    @Test
    void testConvertNull() {
        final var result = new DynamodbPropertyToVarcharColumnValueExtractor(TO_STRING_PROPERTY_TO_COLUMN_MAPPING)
                .mapStringValue(new DynamodbNull());
        assertAll(//
                () -> assertThat(result.isConverted(), equalTo(false)),
                () -> assertThat(result.getValue(), equalTo(null))//
        );
    }

    @Test
    void testConvertList() {
        final DynamodbPropertyToVarcharColumnValueExtractor valueExtractor = new DynamodbPropertyToVarcharColumnValueExtractor(
                TO_STRING_PROPERTY_TO_COLUMN_MAPPING);
        final DynamodbList dynamodbList = new DynamodbList(Collections.emptyList());
        final var result = valueExtractor.mapStringValue(dynamodbList);
        assertThat(result, equalTo(null));
    }
}
