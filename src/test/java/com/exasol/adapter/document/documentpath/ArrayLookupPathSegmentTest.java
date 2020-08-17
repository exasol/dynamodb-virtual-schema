package com.exasol.adapter.document.documentpath;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

import org.junit.jupiter.api.Test;

class ArrayLookupPathSegmentTest {
    private static final ArrayLookupPathSegment TEST_SEGMENT = new ArrayLookupPathSegment(1);
    private static final ArrayLookupPathSegment EQUAL_SEGMENT = new ArrayLookupPathSegment(1);
    private static final ArrayLookupPathSegment UNEQUAL_SEGMENT = new ArrayLookupPathSegment(2);

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