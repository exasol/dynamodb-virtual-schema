package com.exasol.adapter.dynamodb.queryplan;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

import java.util.List;

import org.junit.jupiter.api.Test;

class OrPredicateTest {
    private static final NoPredicate<Object> NO_PREDICATE = new NoPredicate<>();
    private static final OrPredicate<Object> TEST_PREDICATE = new OrPredicate<>(List.of(NO_PREDICATE));

    @Test
    void testGetOredPredicates() {
        assertThat(TEST_PREDICATE.getOredPredicates(), containsInAnyOrder(NO_PREDICATE));
    }

    @Test
    void testVisitor() {
        final PredicateTestVisitor visitor = new PredicateTestVisitor();
        TEST_PREDICATE.accept(visitor);
        assertThat(visitor.getVisited(), equalTo(PredicateTestVisitor.Visited.OR));
    }
}