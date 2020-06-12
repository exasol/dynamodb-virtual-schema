package com.exasol.adapter.dynamodb.querypredicate;

import static com.exasol.EqualityMatchers.assertSymmetricEqualWithHashAndEquals;
import static com.exasol.EqualityMatchers.assertSymmetricNotEqualWithHashAndEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.jupiter.api.Test;

class NotPredicateTest {
    private static final NotPredicate<Object> TEST_PREDICATE = new NotPredicate<>(new NoPredicate<>());

    @Test
    void testVisitor() {
        final PredicateTestVisitor visitor = new PredicateTestVisitor();
        TEST_PREDICATE.accept(visitor);
        assertThat(visitor.getVisited(), equalTo(PredicateTestVisitor.Visited.NOT));
    }

    @Test
    void testToString() {
        assertThat(TEST_PREDICATE.toString(), equalTo("not( NoPredicate )"));
    }

    @Test
    void testSimplify() {
        assertThat(TEST_PREDICATE.simplify(), equalTo(TEST_PREDICATE));
    }

    @Test
    void testIdentical() {
        final NotPredicate<Object> otherPredicate = new NotPredicate<>(TEST_PREDICATE);
        assertSymmetricEqualWithHashAndEquals(TEST_PREDICATE, TEST_PREDICATE);
    }

    @Test
    void testEqual() {
        final NotPredicate<Object> otherPredicate = new NotPredicate<>(new NoPredicate<>());
        assertSymmetricEqualWithHashAndEquals(TEST_PREDICATE, otherPredicate);
    }

    @Test
    void testNotEqual() {
        final NotPredicate<Object> otherPredicate = new NotPredicate<>(TEST_PREDICATE);
        assertSymmetricNotEqualWithHashAndEquals(TEST_PREDICATE, otherPredicate);
    }
}