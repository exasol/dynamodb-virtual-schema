package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;

class DocumentPathToDynamodbExpressionConverterTest {
    private static final DocumentPathToDynamodbExpressionConverter CONVERTER = DocumentPathToDynamodbExpressionConverter
            .getInstance();

    @Test
    void testConvertPath() {
        final DocumentPathExpression path = DocumentPathExpression.builder().addObjectLookup("key")
                .addArrayLookup(2).addObjectLookup("nestedKey").build();
        final DynamodbAttributeNamePlaceholderMapBuilder namePlaceholderMapBuilder = new DynamodbAttributeNamePlaceholderMapBuilder();
        final String result = CONVERTER.convert(path, namePlaceholderMapBuilder);

        assertAll(//
                () -> assertThat(result, equalTo("#0[2].#1")),
                () -> assertThat(namePlaceholderMapBuilder.getPlaceholderMap(),
                        equalTo(Map.of("#0", "key", "#1", "nestedKey")))//
        );
    }

    @Test
    void testArrayAllException() {
        final DocumentPathExpression path = DocumentPathExpression.builder().addArrayAll().build();
        final DynamodbAttributeNamePlaceholderMapBuilder namePlaceholderMapBuilder = new DynamodbAttributeNamePlaceholderMapBuilder();
        final UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class,
                () -> CONVERTER.convert(path, namePlaceholderMapBuilder));
        assertThat(exception.getMessage(),
                equalTo("ArrayAll path segments can't be converted to DynamoDB path expressions."));
    }
}