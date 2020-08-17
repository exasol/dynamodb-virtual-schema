package com.exasol.adapter.document.querypredicate;

import static com.exasol.adapter.document.querypredicate.AbstractComparisonPredicate.Operator.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.document.mapping.ColumnMapping;

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

    private static class StubComparisonPredicate extends AbstractComparisonPredicate {
        private static final long serialVersionUID = -952505391543051812L;

        public StubComparisonPredicate(final Operator operator) {
            super(operator);
        }

        @Override
        public void accept(final ComparisonPredicateVisitor visitor) {

        }

        @Override
        public List<ColumnMapping> getComparedColumns() {
            return null;
        }

        @Override
        public ComparisonPredicate negate() {
            return new StubComparisonPredicate(negateOperator());
        }

        @Override
        public void accept(final QueryPredicateVisitor visitor) {

        }
    }
}