package com.exasol.adapter.dynamodb.documentpath;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.junit.jupiter.api.Test;

public class DocumentPathExpressionTest {

    private static final DocumentPathExpression TEST_PATH = new DocumentPathExpression.Builder().addObjectLookup("test")
            .build();
    private static final DocumentPathExpression EQUAL_PATH = new DocumentPathExpression.Builder()
            .addObjectLookup("test").build();
    private static final DocumentPathExpression OTHER_PATH = new DocumentPathExpression.Builder()
            .addObjectLookup("other").build();

    @Test
    void testEmpty() {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder().build();
        assertThat(pathExpression.size(), equalTo(0));
    }

    @Test
    void testAdd() {
        final ObjectLookupPathSegment pathSegment1 = new ObjectLookupPathSegment("key");
        final ObjectLookupPathSegment pathSegment2 = new ObjectLookupPathSegment("key2");
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder()//
                .addPathSegment(pathSegment1)//
                .addPathSegment(pathSegment2)//
                .build();
        assertAll(//
                () -> assertThat(pathExpression.size(), equalTo(2)),
                () -> assertThat(pathExpression.getSegments().get(0), equalTo(pathSegment1)), //
                () -> assertThat(pathExpression.getSegments().get(1), equalTo(pathSegment2))//
        );
    }

    @Test
    void testAddObjectLookup() {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder()//
                .addObjectLookup("key")//
                .build();
        final ObjectLookupPathSegment objectLookup = (ObjectLookupPathSegment) pathExpression.getSegments().get(0);
        assertThat(objectLookup.getLookupKey(), equalTo("key"));
    }

    @Test
    void testAddArrayLookup() {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder()//
                .addArrayLookup(0).build();
        final ArrayLookupPathSegment objectLookup = (ArrayLookupPathSegment) pathExpression.getSegments().get(0);
        assertThat(objectLookup.getLookupIndex(), equalTo(0));
    }

    @Test
    void testSubPath() {
        final ObjectLookupPathSegment pathSegment1 = new ObjectLookupPathSegment("key");
        final ObjectLookupPathSegment pathSegment2 = new ObjectLookupPathSegment("key2");
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder()//
                .addPathSegment(pathSegment1)//
                .addPathSegment(pathSegment2)//
                .build().getSubPath(0, 1);
        assertAll(//
                () -> assertThat(pathExpression.size(), equalTo(1)),
                () -> assertThat(pathExpression.getSegments().get(0), equalTo(pathSegment1))//
        );
    }

    @Test
    void testEquality() {
        assertThat(TEST_PATH, equalTo(EQUAL_PATH));
    }

    @Test
    void testHashEquality() {
        assertThat(TEST_PATH.hashCode(), equalTo(EQUAL_PATH.hashCode()));
    }

    @Test
    void testInequality() {
        assertThat(TEST_PATH, not(equalTo(OTHER_PATH)));
    }

    @Test
    void testHashInequality() {
        assertThat(TEST_PATH.hashCode(), not(equalTo(OTHER_PATH.hashCode())));
    }
}
