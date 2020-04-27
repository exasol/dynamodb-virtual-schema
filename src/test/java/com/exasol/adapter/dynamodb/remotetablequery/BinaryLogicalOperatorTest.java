package com.exasol.adapter.dynamodb.remotetablequery;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

import java.util.List;

import org.junit.jupiter.api.Test;

class BinaryLogicalOperatorTest {
    private static final NoPredicate<Object> NO_PREDICATE = new NoPredicate<>();
    private static final BinaryLogicalOperator.Operator OPERATOR = BinaryLogicalOperator.Operator.AND;
    private static final BinaryLogicalOperator<Object> TEST_PREDICATE = new BinaryLogicalOperator<>(
            List.of(NO_PREDICATE), OPERATOR);

    @Test
    void testGetAndedPredicates() {
        assertThat(TEST_PREDICATE.getOperands(), containsInAnyOrder(NO_PREDICATE));
    }

    void testGetOperator() {
        assertThat(TEST_PREDICATE.getOperator(), equalTo(OPERATOR));
    }

    @Test
    void testVisitor() {
        final PredicateTestVisitor visitor = new PredicateTestVisitor();
        TEST_PREDICATE.accept(visitor);
        assertThat(visitor.getVisited(), equalTo(PredicateTestVisitor.Visited.BINARY_LOGICAL_OPERATOR));
    }
}