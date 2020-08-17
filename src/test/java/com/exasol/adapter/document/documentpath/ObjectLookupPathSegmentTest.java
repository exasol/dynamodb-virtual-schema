package com.exasol.adapter.document.documentpath;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

import org.junit.jupiter.api.Test;

class ObjectLookupPathSegmentTest {
    private static final ObjectLookupPathSegment TEST_SEGMENT = new ObjectLookupPathSegment("test");
    private static final ObjectLookupPathSegment EQUAL_SEGMENT = new ObjectLookupPathSegment("test");
    private static final ObjectLookupPathSegment UNEQUAL_SEGMENT = new ObjectLookupPathSegment("other");

    @Test
    void testEquality() {
        assertThat(TEST_SEGMENT, equalTo(EQUAL_SEGMENT));
    }

    @Test
    void testInequality() {
        assertThat(TEST_SEGMENT, not(equalTo(UNEQUAL_SEGMENT)));
    }

    @Test
    void testEqualHash() {
        assertThat(TEST_SEGMENT.hashCode(), equalTo(EQUAL_SEGMENT.hashCode()));
    }

    @Test
    void testUnequalHash() {
        assertThat(TEST_SEGMENT.hashCode(), not(equalTo(UNEQUAL_SEGMENT.hashCode())));
    }
}