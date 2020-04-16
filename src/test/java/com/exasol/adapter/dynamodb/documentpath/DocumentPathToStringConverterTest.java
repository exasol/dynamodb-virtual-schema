package com.exasol.adapter.dynamodb.documentpath;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.jupiter.api.Test;

public class DocumentPathToStringConverterTest {

    @Test
    void testStringifyEmpty() {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder().build();
        final String result = new DocumentPathToStringConverter().convertToString(pathExpression);
        assertThat(result, equalTo("/"));
    }

    @Test
    void testStringifyObjectLookup() {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder()//
                .addObjectLookup("key1")//
                .addObjectLookup("key2")//
                .build();
        final String result = new DocumentPathToStringConverter().convertToString(pathExpression);
        assertThat(result, equalTo("/key1/key2"));
    }

    @Test
    void testStringifyArrayLookup() {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder()//
                .addObjectLookup("key1")//
                .addArrayLookup(0)//
                .build();
        final String result = new DocumentPathToStringConverter().convertToString(pathExpression);
        assertThat(result, equalTo("/key1[0]"));
    }

    @Test
    void testStringifyArrayAll() {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder()//
                .addObjectLookup("key1")//
                .addArrayAll()//
                .build();
        final String result = new DocumentPathToStringConverter().convertToString(pathExpression);
        assertThat(result, equalTo("/key1[*]"));
    }

}
