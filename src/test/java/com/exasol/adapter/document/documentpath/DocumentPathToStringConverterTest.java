package com.exasol.adapter.document.documentpath;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.jupiter.api.Test;

class DocumentPathToStringConverterTest {

    @Test
    void testStringifyEmpty() {
        final DocumentPathExpression pathExpression = DocumentPathExpression.empty();
        final String result = new DocumentPathToStringConverter().convertToString(pathExpression);
        assertThat(result, equalTo("/"));
    }

    @Test
    void testStringifyObjectLookup() {
        final DocumentPathExpression pathExpression = DocumentPathExpression.builder()//
                .addObjectLookup("key1")//
                .addObjectLookup("key2")//
                .build();
        final String result = new DocumentPathToStringConverter().convertToString(pathExpression);
        assertThat(result, equalTo("/key1/key2"));
    }

    @Test
    void testStringifyArrayLookup() {
        final DocumentPathExpression pathExpression = DocumentPathExpression.builder()//
                .addObjectLookup("key1")//
                .addArrayLookup(0)//
                .build();
        final String result = new DocumentPathToStringConverter().convertToString(pathExpression);
        assertThat(result, equalTo("/key1[0]"));
    }

    @Test
    void testStringifyArrayAll() {
        final DocumentPathExpression pathExpression = DocumentPathExpression.builder()//
                .addObjectLookup("key1")//
                .addArrayAll()//
                .build();
        final String result = new DocumentPathToStringConverter().convertToString(pathExpression);
        assertThat(result, equalTo("/key1[*]"));
    }

}
