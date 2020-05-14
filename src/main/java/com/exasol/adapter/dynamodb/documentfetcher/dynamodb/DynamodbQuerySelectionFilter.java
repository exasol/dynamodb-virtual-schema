package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import java.util.ArrayList;
import java.util.List;

import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.dynamodb.mapping.ColumnMapping;
import com.exasol.adapter.dynamodb.mapping.PropertyToColumnMapping;
import com.exasol.adapter.dynamodb.remotetablequery.*;

/**
 * This class filters the {@link ColumnLiteralComparisonPredicate}s of a query using a whitelist that contains the
 * allowed names of attributes that are part of the column. By that a query can be filtered to only contain comparisons
 * on the columns defined in the whitelist. This can for example be needed for extracting the selection on key columns
 * of a table. The filter transforms the selection so that the result space of the query might get increased but never
 * gets decreased.
 */
class DynamodbQuerySelectionFilter {

    /**
     * Filters a selection using a attributeNameWhitelist for attributes to be part of predicates in the query.
     * 
     * @param selection              input selection to be filtered
     * @param attributeNameWhitelist list of attribute names. All comparisons on other attributes are filtered out.
     * @return selection containing only predicates on the whitelisted attributes.
     */
    public QueryPredicate<DynamodbNodeVisitor> filter(final QueryPredicate<DynamodbNodeVisitor> selection,
            final List<String> attributeNameWhitelist) {
        final FilterVisitor filterVisitor = new FilterVisitor(attributeNameWhitelist, false);
        selection.accept(filterVisitor);
        return filterVisitor.getFiltered();
    }

    private static class FilterVisitor implements QueryPredicateVisitor<DynamodbNodeVisitor> {
        private final List<String> attributeNameWhitelist;
        private final boolean isNegated;
        private QueryPredicate<DynamodbNodeVisitor> filtered;

        private FilterVisitor(final List<String> attributeNameWhitelist, final boolean isNegated) {
            this.attributeNameWhitelist = attributeNameWhitelist;
            this.isNegated = isNegated;
        }

        @Override
        public void visit(
                final ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> columnLiteralComparisonPredicate) {
            final ColumnMapping column = columnLiteralComparisonPredicate.getColumn();
            if (column instanceof PropertyToColumnMapping
                    && isColumnOnWhitelist(((PropertyToColumnMapping) column).getPathToSourceProperty())) {
                this.filtered = columnLiteralComparisonPredicate;
            } else {
                this.filtered = new NoPredicate<>();
            }
        }

        private boolean isColumnOnWhitelist(final DocumentPathExpression columnsPath) {
            for (final String whitelistedProperty : this.attributeNameWhitelist) {
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
            final List<QueryPredicate<DynamodbNodeVisitor>> filteredOperands = filterOperands(logicalOperator,
                    operator);
            if (filteredOperands.isEmpty()) {
                this.filtered = new NoPredicate<>();
            } else if (filteredOperands.size() == 1) {
                this.filtered = filteredOperands.get(0);
            } else {
                this.filtered = new LogicalOperator<>(filteredOperands, operator);
            }
        }

        private List<QueryPredicate<DynamodbNodeVisitor>> filterOperands(
                final LogicalOperator<DynamodbNodeVisitor> logicalOperator, final LogicalOperator.Operator operator) {
            final List<QueryPredicate<DynamodbNodeVisitor>> filteredOperands = new ArrayList<>();
            for (final QueryPredicate<DynamodbNodeVisitor> operand : logicalOperator.getOperands()) {
                final QueryPredicate<DynamodbNodeVisitor> filteredOperand = filterOperand(operand);
                if (!(filteredOperand instanceof NoPredicate)) {
                    filteredOperands.add(filteredOperand);
                } else if (!this.isNegated && operator.equals(LogicalOperator.Operator.OR)
                        || this.isNegated && operator.equals(LogicalOperator.Operator.AND)) {
                    throw new DynamodbQuerySelectionFilterException(
                            "The key predicates of this plan could not be extracted without potentially loosing results of this query."
                                    + " Please simplify the query or use a different DynamoDB operation.");
                }
            }
            return filteredOperands;
        }

        private QueryPredicate<DynamodbNodeVisitor> filterOperand(final QueryPredicate<DynamodbNodeVisitor> operand) {
            final FilterVisitor filter = new FilterVisitor(this.attributeNameWhitelist, this.isNegated);
            operand.accept(filter);
            return filter.filtered;
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
