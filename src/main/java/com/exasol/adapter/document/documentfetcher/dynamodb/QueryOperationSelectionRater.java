package com.exasol.adapter.document.documentfetcher.dynamodb;

import java.util.Optional;

import com.exasol.adapter.document.querypredicate.ColumnLiteralComparisonPredicate;
import com.exasol.errorreporting.ExaError;

/**
 * This class rates the selectivity of a {@link QueryOperationSelection}.
 */
public class QueryOperationSelectionRater {
    /**
     * Rate the selectivity of a {@link QueryOperationSelection}.
     * 
     * @param selection {@link QueryOperationSelection} to rate
     * @return selectivity rating. High value means high selectivity.
     */
    public int rate(final QueryOperationSelection selection) {
        final Optional<ColumnLiteralComparisonPredicate> sortKeyCondition = selection.getSortKeyCondition();
        if (sortKeyCondition.isEmpty()) {
            if (selection.getIndex().hasSortKey()) {
                return 0;
            } else {
                return 4;
            }
        } else {
            return rateOperator(sortKeyCondition.get());
        }
    }

    private int rateOperator(final ColumnLiteralComparisonPredicate sortKeyCondition) {
        switch (sortKeyCondition.getOperator()) {
        case EQUAL:
            return 3;
        case NOT_EQUAL:
            return 0;
        case LESS:
        case LESS_EQUAL:
        case GREATER:
        case GREATER_EQUAL:
            return 1;
        default:
            throw new UnsupportedOperationException(ExaError.messageBuilder("E-VSDY-27")
                    .message("Operator rating is not implemented for the operator {{operator}}",
                            sortKeyCondition.getOperator())
                    .toString());
        }
    }
}
