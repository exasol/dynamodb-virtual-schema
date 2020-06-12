package com.exasol.adapter.dynamodb.querypredicate;

import static com.exasol.adapter.dynamodb.querypredicate.AbstractComparisonPredicate.Operator.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.mapping.ColumnMapping;

class AbstractComparisonPredicateTest {

    @Test
    void testNegateEquality() {
        final List<AbstractComparisonPredicate.Operator> operators = List.of(EQUAL, LESS, LESS_EQUAL, GREATER,
                GREATER_EQUAL, NOT_EQUAL);
        for (final AbstractComparisonPredicate.Operator operator : operators) {
            final StubComparisonPredicate comparisonPredicate = new StubComparisonPredicate(operator);
            assertThat(comparisonPredicate.getOperator(), equalTo(comparisonPredicate.negate().negate().getOperator()));
        }
    }

    private static class StubComparisonPredicate extends AbstractComparisonPredicate<Object> {
        private static final long serialVersionUID = -952505391543051812L;

        public StubComparisonPredicate(final Operator operator) {
            super(operator);
        }

        @Override
        public void accept(final ComparisonPredicateVisitor<Object> visitor) {

        }

        @Override
        public List<ColumnMapping> getComparedColumns() {
            return null;
        }

        @Override
        public ComparisonPredicate<Object> negate() {
            return new StubComparisonPredicate(negateOperator());
        }

        @Override
        public void accept(final QueryPredicateVisitor<Object> visitor) {

        }
    }
}