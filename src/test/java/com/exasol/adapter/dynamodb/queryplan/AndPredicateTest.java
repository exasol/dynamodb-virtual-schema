package com.exasol.adapter.dynamodb.queryplan;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

import java.util.List;

import org.junit.jupiter.api.Test;

class AndPredicateTest {
    private static final NoPredicate<Object> NO_PREDICATE = new NoPredicate<>();
    private static final AndPredicate<Object> TEST_PREDICATE = new AndPredicate<>(List.of(NO_PREDICATE));

    @Test
    void testGetAndedPredicates() {
        assertThat(TEST_PREDICATE.getAndedPredicates(), containsInAnyOrder(NO_PREDICATE));
    }

    @Test
    void testVisitor() {
        final PredicateTestVisitor visitor = new PredicateTestVisitor();
        TEST_PREDICATE.accept(visitor);
        assertThat(visitor.getVisited(), equalTo(PredicateTestVisitor.Visited.AND));
    }
}