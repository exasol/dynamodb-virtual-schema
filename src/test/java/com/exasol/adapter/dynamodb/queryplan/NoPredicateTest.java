package com.exasol.adapter.dynamodb.queryplan;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.jupiter.api.Test;

class NoPredicateTest {
    private static final NoPredicate<Object> TEST_PREDICATE = new NoPredicate<>();

    @Test
    void testVisitor() {
        final PredicateTestVisitor visitor = new PredicateTestVisitor();
        TEST_PREDICATE.accept(visitor);
        assertThat(visitor.getVisited(), equalTo(PredicateTestVisitor.Visited.NO));
    }
}