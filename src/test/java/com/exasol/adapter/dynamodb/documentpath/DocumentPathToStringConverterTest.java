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
    void testStringifyObject() {
        final ObjectPathSegment pathSegment1 = new ObjectPathSegment("key1");
        final ObjectPathSegment pathSegment2 = new ObjectPathSegment("key2");
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder()//
                .add(pathSegment1)//
                .add(pathSegment2)//
                .build();
        final String result = new DocumentPathToStringConverter().convertToString(pathExpression);
        assertThat(result, equalTo("/key1/key2/"));
    }

}
