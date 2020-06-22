package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;

class DynamodbProjectionExpressionFactoryTest {
    private static final DynamodbProjectionExpressionFactory FACTORY = new DynamodbProjectionExpressionFactory();

    @Test
    void testEmpty() {
        final DynamodbAttributeNamePlaceholderMapBuilder namePlaceholderMapBuilder = new DynamodbAttributeNamePlaceholderMapBuilder();
        final String result = FACTORY.build(Collections.emptyList(), namePlaceholderMapBuilder);
        assertAll(//
                () -> assertThat(result, is(emptyString())),
                () -> assertThat(namePlaceholderMapBuilder.getPlaceholderMap(), is(anEmptyMap()))//
        );
    }

    @Test
    void testSingleEntry() {
        final DynamodbAttributeNamePlaceholderMapBuilder namePlaceholderMapBuilder = new DynamodbAttributeNamePlaceholderMapBuilder();
        final String result = FACTORY.build(
                List.of(new DocumentPathExpression.Builder().addObjectLookup("isbn").build()),
                namePlaceholderMapBuilder);
        assertAll(//
                () -> assertThat(result, equalTo("#0")),
                () -> assertThat(namePlaceholderMapBuilder.getPlaceholderMap(), equalTo(Map.of("#0", "isbn")))//
        );
    }

    @Test
    void testTwoEntries() {
        final DynamodbAttributeNamePlaceholderMapBuilder namePlaceholderMapBuilder = new DynamodbAttributeNamePlaceholderMapBuilder();
        final String result = FACTORY
                .build(List.of(new DocumentPathExpression.Builder().addObjectLookup("isbn").build(),
                        new DocumentPathExpression.Builder().addObjectLookup("authors").addArrayAll()
                                .addObjectLookup("name").build()),
                        namePlaceholderMapBuilder);
        assertAll(//
                () -> assertThat(result, equalTo("#0, #1")),
                () -> assertThat(namePlaceholderMapBuilder.getPlaceholderMap(),
                        equalTo(Map.of("#0", "isbn", "#1", "authors")))//
        );
    }
}