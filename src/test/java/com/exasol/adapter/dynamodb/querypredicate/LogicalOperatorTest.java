package com.exasol.adapter.dynamodb.querypredicate;

import static com.exasol.EqualityMatchers.assertSymmetricEqualWithHashAndEquals;
import static com.exasol.EqualityMatchers.assertSymmetricNotEqualWithHashAndEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

import java.util.Set;

import org.junit.jupiter.api.Test;

class LogicalOperatorTest {
    private static final NoPredicate NO_PREDICATE = new NoPredicate();
    private static final LogicalOperator.Operator OPERATOR = LogicalOperator.Operator.AND;
    private static final LogicalOperator TEST_PREDICATE = new LogicalOperator(Set.of(NO_PREDICATE),
            OPERATOR);

    @Test
    void testGetAndedPredicates() {
        assertThat(TEST_PREDICATE.getOperands(), containsInAnyOrder(NO_PREDICATE));
    }

    @Test
    void testGetOperator() {
        assertThat(TEST_PREDICATE.getOperator(), equalTo(OPERATOR));
    }

    @Test
    void testVisitor() {
        final PredicateTestVisitor visitor = new PredicateTestVisitor();
        TEST_PREDICATE.accept(visitor);
        assertThat(visitor.getVisited(), equalTo(PredicateTestVisitor.Visited.BINARY_LOGICAL_OPERATOR));
    }

    @Test
    void testNotEqual() {
        final LogicalOperator otherPredicate = new LogicalOperator(Set.of(NO_PREDICATE),
                LogicalOperator.Operator.OR);
        assertSymmetricNotEqualWithHashAndEquals(TEST_PREDICATE, otherPredicate);
    }

    @Test
    void testIdentical() {
        final LogicalOperator otherPredicate = new LogicalOperator(Set.of(NO_PREDICATE),
                LogicalOperator.Operator.OR);
        assertSymmetricEqualWithHashAndEquals(TEST_PREDICATE, TEST_PREDICATE);
    }

    @Test
    void testEqual() {
        final LogicalOperator otherPredicate = new LogicalOperator(Set.of(NO_PREDICATE), OPERATOR);
        assertSymmetricEqualWithHashAndEquals(TEST_PREDICATE, otherPredicate);
    }
}