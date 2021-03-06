package com.exasol.adapter.document.documentfetcher.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.document.documentpath.DocumentPathExpression;

class DynamodbProjectionExpressionFactoryTest {
    private static final DynamodbProjectionExpressionFactory FACTORY = new DynamodbProjectionExpressionFactory();
    public static final DocumentPathExpression ISBN_PATH = DocumentPathExpression.builder().addObjectLookup("isbn")
            .build();
    public static final DocumentPathExpression PUBLISHER_PATH = DocumentPathExpression.builder()
            .addObjectLookup("publisher").build();
    public static final DocumentPathExpression PUBLISHER_NAME_PATH = DocumentPathExpression.builder()
            .addObjectLookup("publisher").addObjectLookup("name").build();
    public static final DocumentPathExpression AUTHORS_NAME_PATH = DocumentPathExpression.builder()
            .addObjectLookup("authors").addArrayAll().addObjectLookup("name").build();

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
        final String result = FACTORY.build(List.of(PUBLISHER_NAME_PATH), namePlaceholderMapBuilder);
        assertAll(//
                () -> assertThat(result, equalTo("#0.#1")),
                () -> assertThat(namePlaceholderMapBuilder.getPlaceholderMap(),
                        equalTo(Map.of("#0", "publisher", "#1", "name")))//
        );
    }

    @Test
    void testTwoEntries() {
        final DynamodbAttributeNamePlaceholderMapBuilder namePlaceholderMapBuilder = new DynamodbAttributeNamePlaceholderMapBuilder();
        final String result = FACTORY.build(List.of(ISBN_PATH, AUTHORS_NAME_PATH), namePlaceholderMapBuilder);
        assertAll(//
                () -> assertThat(result, equalTo("#0, #1")),
                () -> assertThat(namePlaceholderMapBuilder.getPlaceholderMap(),
                        equalTo(Map.of("#0", "isbn", "#1", "authors")))//
        );
    }
}