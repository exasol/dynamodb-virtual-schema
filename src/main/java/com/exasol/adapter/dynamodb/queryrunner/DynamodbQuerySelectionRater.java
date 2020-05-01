package com.exasol.adapter.dynamodb.queryrunner;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.remotetablequery.*;

/**
 * This class rates how selective a query is.
 */
public class DynamodbQuerySelectionRater {
    public static final int RATING_EQUALITY = 2;
    public static final int RATING_RANGE = 1;
    public static final int RATING_NO_SELECTIVITY = 0;

    /**
     * Calculates the a rating for a given selection.
     * 
     * @param selection selection that is rated.
     * @return selectivity rating. Use {@link #RATING_EQUALITY}, {@link #RATING_RANGE} or {@link #RATING_NO_SELECTIVITY}
     *         to compare.
     */
    public int rate(final QueryPredicate<DynamodbNodeVisitor> selection) {
        final RatingVisitor ratingVisitor = new RatingVisitor();
        selection.accept(ratingVisitor);
        return ratingVisitor.getRating();
    }

    private static class RatingVisitor implements QueryPredicateVisitor<DynamodbNodeVisitor> {
        private int rating;

        @Override
        public void visit(
                final ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> columnLiteralComparisonPredicate) {
            switch (columnLiteralComparisonPredicate.getOperator()) {
            case EQUAL:
                this.rating = RATING_EQUALITY;
                break;
            default:
                throw new UnsupportedOperationException("This operator was not yet implemented.");
            }
        }

        @Override
        public void visit(final LogicalOperator<DynamodbNodeVisitor> logicalOperator) {
            final List<Integer> operandsRatings = logicalOperator.getOperands().stream()
                    .map(operand -> new DynamodbQuerySelectionRater().rate(operand)).collect(Collectors.toList());
            if (logicalOperator.getOperator().equals(LogicalOperator.Operator.AND)) {
                this.rating = Collections.max(operandsRatings);
            } else {// OR
                this.rating = Collections.min(operandsRatings);
            }
        }

        @Override
        public void visit(final NoPredicate<DynamodbNodeVisitor> noPredicate) {
            this.rating = RATING_NO_SELECTIVITY;
        }

        public int getRating() {
            return this.rating;
        }
    }
}
