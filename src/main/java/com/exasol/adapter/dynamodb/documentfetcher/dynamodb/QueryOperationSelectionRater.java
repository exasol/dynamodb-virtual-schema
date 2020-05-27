package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import java.util.Optional;

import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.remotetablequery.ColumnLiteralComparisonPredicate;

/**
 * This class rates the selectivity of a {@link QueryOperationSelection}.
 */
public class QueryOperationSelectionRater {
//TODO improved to detect if key is unique
    /**
     * Rate the selectivity of a {@link QueryOperationSelection}.
     * 
     * @param selection {@link QueryOperationSelection} to rate
     * @return selectivity rating. High value means high selectivity.
     */
    public int rate(final QueryOperationSelection selection) {
        final Optional<ColumnLiteralComparisonPredicate<DynamodbNodeVisitor>> sortKeyCondition = selection
                .getSortKeyCondition();
        if (sortKeyCondition.isEmpty()) {
            return 0;
        } else {
            switch (sortKeyCondition.get().getOperator()) {
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
                throw new UnsupportedOperationException("This operator is not yet implemented.");
            }
        }
    }
}
