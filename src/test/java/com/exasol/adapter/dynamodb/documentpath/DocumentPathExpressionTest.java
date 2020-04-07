package com.exasol.adapter.dynamodb.documentpath;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link DocumentPathExpression}
 */
public class DocumentPathExpressionTest {

    @Test
    void testEmpty() {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder().build();
        assertThat(pathExpression.size(), equalTo(0));
    }

    @Test
    void testAdd() {
        final ObjectPathSegment pathSegment1 = new ObjectPathSegment("key");
        final ObjectPathSegment pathSegment2 = new ObjectPathSegment("key2");
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder()//
                .add(pathSegment1)//
                .add(pathSegment2)//
                .build();
        assertAll(//
                () -> assertThat(pathExpression.size(), equalTo(2)),
                () -> assertThat(pathExpression.getPath().get(0), equalTo(pathSegment1)), //
                () -> assertThat(pathExpression.getPath().get(1), equalTo(pathSegment2))//
        );
    }

    @Test
    void testSubpath() {
        final ObjectPathSegment pathSegment1 = new ObjectPathSegment("key");
        final ObjectPathSegment pathSegment2 = new ObjectPathSegment("key2");
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder()//
                .add(pathSegment1)//
                .add(pathSegment2)//
                .build().getSubPath(0, 1);
        assertAll(//
                () -> assertThat(pathExpression.size(), equalTo(1)),
                () -> assertThat(pathExpression.getPath().get(0), equalTo(pathSegment1))//
        );
    }
}
