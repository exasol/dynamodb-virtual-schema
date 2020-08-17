package com.exasol.adapter.document.querypredicate;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.jupiter.api.Test;

class NoPredicateTest {
    private static final NoPredicate TEST_PREDICATE = new NoPredicate();

    @Test
    void testVisitor() {
        final PredicateTestVisitor visitor = new PredicateTestVisitor();
        TEST_PREDICATE.accept(visitor);
        assertThat(visitor.getVisited(), equalTo(PredicateTestVisitor.Visited.NO));
    }

    @Test
    void testSimplify() {
        assertThat(TEST_PREDICATE.simplify(), equalTo(TEST_PREDICATE));
    }

    @Test
    void testToString() {
        assertThat(TEST_PREDICATE.toString(), equalTo("NoPredicate"));
    }
}