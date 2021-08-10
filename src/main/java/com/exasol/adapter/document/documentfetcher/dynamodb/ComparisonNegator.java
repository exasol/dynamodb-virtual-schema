package com.exasol.adapter.document.documentfetcher.dynamodb;

import static com.exasol.adapter.document.querypredicate.AbstractComparisonPredicate.Operator.*;

import com.exasol.adapter.document.querypredicate.*;
import com.exasol.adapter.document.querypredicate.AbstractComparisonPredicate.Operator;
import com.exasol.errorreporting.ExaError;

/**
 * This class negates a {@link ComparisonPredicate}.
 */
public class ComparisonNegator {

    /**
     * Negate a {@link ComparisonPredicate}.
     *
     * @param predicate comparison predicate to negate
     * @return negated comparison.
     */
    public ComparisonPredicate negate(final ComparisonPredicate predicate) {
        final NegatingVisitor visitor = new NegatingVisitor();
        predicate.accept(visitor);
        return visitor.getNegated();
    }

    private static class NegatingVisitor implements ComparisonPredicateVisitor {
        private ComparisonPredicate negated;

        @Override
        public void visit(final ColumnLiteralComparisonPredicate predicate) {
            this.negated = new ColumnLiteralComparisonPredicate(negateOperator(predicate.getOperator()),
                    predicate.getColumn(), predicate.getLiteral());
        }

        private Operator negateOperator(final Operator operator) {
            switch (operator) {
            case EQUAL:
                return NOT_EQUAL;
            case NOT_EQUAL:
                return EQUAL;
            case LESS:
                return GREATER_EQUAL;
            case LESS_EQUAL:
                return GREATER;
            case GREATER:
                return LESS_EQUAL;
            case GREATER_EQUAL:
                return LESS;
            case LIKE:
                return NOT_LIKE;
            case NOT_LIKE:
                return LIKE;
            default:
                throw new UnsupportedOperationException(ExaError.messageBuilder("E-VS-DY-2")
                        .message("Unsupported operator {{operator}}.", operator).ticketMitigation().toString());// All
                                                                                                                // possible
                                                                                                                // operators
                                                                                                                // are
                                                                                                                // implemented
                                                                                                                // and
                                                                                                                // checked
                                                                                                                // by
            // unit-test
            }
        }

        public ComparisonPredicate getNegated() {
            return this.negated;
        }
    }
}
