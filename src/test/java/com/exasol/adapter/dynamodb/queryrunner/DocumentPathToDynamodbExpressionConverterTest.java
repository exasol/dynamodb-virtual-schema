package com.exasol.adapter.dynamodb.queryrunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;

class DocumentPathToDynamodbExpressionConverterTest {

    @Test
    void testConvertPath() {
        final DocumentPathExpression path = new DocumentPathExpression.Builder().addObjectLookup("key")
                .addArrayLookup(2).addObjectLookup("nestedKey").build();
        final String result = new DocumentPathToDynamodbExpressionConverter().convert(path);
        assertThat(result, equalTo("key[2].nestedKey"));
    }

    @Test
    void testArrayAllException() {
        final DocumentPathExpression path = new DocumentPathExpression.Builder().addArrayAll().build();
        final UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class,
                () -> new DocumentPathToDynamodbExpressionConverter().convert(path));
        assertThat(exception.getMessage(),
                equalTo("ArrayAll path segments can't be converted to DynamoDB path expressions."));
    }
}