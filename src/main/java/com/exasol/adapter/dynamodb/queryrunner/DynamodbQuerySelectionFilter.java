package com.exasol.adapter.dynamodb.queryrunner;

import java.util.ArrayList;
import java.util.List;

import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.dynamodb.remotetablequery.*;

/**
 * This class filters a query's selection using a whitelist so it can be transformed into an KeyConditionExpression for
 * a DynamoDB {@code QUERY} request. By the filtering the result space of the query might get increased but not
 * decreased.
 */
class DynamodbQuerySelectionFilter {

    /**
     * Filters a selection using a whitelist for attributes to be part of predicates in the query.
     * 
     * @param selection input selection to be filtered
     * @param whitelist list of attribute names. All predicates on other attributes are filtered out.
     * @return selection containing only predicates on the whitelisted attributes.
     */
    public QueryPredicate<DynamodbNodeVisitor> filter(final QueryPredicate<DynamodbNodeVisitor> selection,
            final List<String> whitelist) {
        final FilterVisitor filterVisitor = new FilterVisitor(whitelist, false);
            selection.accept(filterVisitor);
        return filterVisitor.getFiltered();
    }

    private static class FilterVisitor implements QueryPredicateVisitor<DynamodbNodeVisitor> {
        private final List<String> whitelist;
        private QueryPredicate<DynamodbNodeVisitor> filtered;
        private final boolean isNegated;

        private FilterVisitor(final List<String> whitelist, final boolean isNegated) {
            this.whitelist = whitelist;
            this.isNegated = isNegated;
        }

        @Override
        public void visit(
                final ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> columnLiteralComparisonPredicate) {
            if (isColumnOnWhitelist(columnLiteralComparisonPredicate.getColumn().getPathToSourceProperty())) {
                this.filtered = columnLiteralComparisonPredicate;
            } else {
                this.filtered = new NoPredicate<>();
            }
        }

        private boolean isColumnOnWhitelist(final DocumentPathExpression columnsPath) {
            for (final String whitelistedProperty : this.whitelist) {
                final DocumentPathExpression pathForWhitelisted = new DocumentPathExpression.Builder()
                        .addObjectLookup(whitelistedProperty).build();
                if (columnsPath.equals(pathForWhitelisted)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void visit(final LogicalOperator<DynamodbNodeVisitor> logicalOperator) {
            final LogicalOperator.Operator operator = logicalOperator.getOperator();
            final List<QueryPredicate<DynamodbNodeVisitor>> inputOperands = logicalOperator.getOperands();
            final List<QueryPredicate<DynamodbNodeVisitor>> filteredOperands = new ArrayList<>();
            for (final QueryPredicate<DynamodbNodeVisitor> operand : inputOperands) {
                final FilterVisitor filter = new FilterVisitor(this.whitelist, this.isNegated);
                operand.accept(filter);
                final QueryPredicate<DynamodbNodeVisitor> filteredOperand = filter.filtered;
                if (!(filteredOperand instanceof NoPredicate)) {
                    filteredOperands.add(filteredOperand);
                } else if (!this.isNegated && operator.equals(LogicalOperator.Operator.OR)
                        || this.isNegated && operator.equals(LogicalOperator.Operator.AND)) {
                    throw new DynamodbQuerySelectionFilterException(
                            "The key predicates of this plan could not be extracted without potentially loosing results of this query."
                                    + " Please simplify the query or use a different DynamoDB operation.");
                }
            }
            if (filteredOperands.isEmpty()) {
                this.filtered = new NoPredicate<>();
            } else if (filteredOperands.size() == 1) {
                this.filtered = filteredOperands.get(0);
            } else {
                this.filtered = new LogicalOperator<>(filteredOperands, operator);
            }
        }

        @Override
        public void visit(final NoPredicate<DynamodbNodeVisitor> noPredicate) {
            this.filtered = noPredicate;
        }

        public QueryPredicate<DynamodbNodeVisitor> getFiltered() {
            return this.filtered;
        }
    }
}
