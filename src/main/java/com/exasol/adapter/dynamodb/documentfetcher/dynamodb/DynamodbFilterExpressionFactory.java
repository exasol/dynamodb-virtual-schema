package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import java.util.List;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeToAttributeValueConverter;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.dynamodb.remotetablequery.*;

/**
 * This class builds a DynamoDB filter expression for a given selection.
 */
public class DynamodbFilterExpressionFactory {

    /**
     * Build a DynamoDB filter expression for the given selection.
     * 
     * @param selection        selection to converted.
     * @param valueListBuilder value list builder that takes the literals form the queries and gives placeholders for
     *                         them.
     * @return DynamoDB filter expression
     */
    public String buildFilterExpression(final QueryPredicate<DynamodbNodeVisitor> selection,
            final DynamodbValueListBuilderAddValueInterface valueListBuilder) {
        final Visitor visitor = new Visitor(valueListBuilder);
        selection.accept(visitor);
        return visitor.getFilterExpression();
    }

    private static class Visitor implements QueryPredicateVisitor<DynamodbNodeVisitor> {
        private final DynamodbValueListBuilderAddValueInterface valueListBuilder;
        private String filterExpression;

        private Visitor(final DynamodbValueListBuilderAddValueInterface valueListBuilder) {
            this.valueListBuilder = valueListBuilder;
        }

        @Override
        public void visit(
                final ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> columnLiteralComparisonPredicate) {
            final DocumentPathExpression columnsPath = columnLiteralComparisonPredicate.getColumn()
                    .getPathToSourceProperty();
            final String columnPathExpression = new DocumentPathToDynamodbExpressionConverter().convert(columnsPath);
            final AttributeValue attributeValue = new DynamodbNodeToAttributeValueConverter()
                    .convertToAttributeValue(columnLiteralComparisonPredicate.getLiteral());
            final String valuePlaceholder = this.valueListBuilder.addValue(attributeValue);
            this.filterExpression = columnPathExpression + " "
                    + convertComparisonOperator(columnLiteralComparisonPredicate.getOperator()) + " "
                    + valuePlaceholder;
        }

        private String convertComparisonOperator(final ComparisonPredicate.Operator operator) {
            switch (operator) {
            case EQUAL:
                return "=";
            case LESS:
                return "<";
            case LESS_EQUAL:
                return "<=";
            case GREATER:
                return ">";
            case GREATER_EQUAL:
                return ">=";
            default:
                throw new UnsupportedOperationException("This operator has no equivalent in DynamoDB.");
            }
        }

        @Override
        public void visit(final LogicalOperator<DynamodbNodeVisitor> logicalOperator) {
            final List<QueryPredicate<DynamodbNodeVisitor>> operands = logicalOperator.getOperands();
            if (operands.size() < 2) {
                throw new IllegalArgumentException(
                        "Logic expressions with only one operand can be must be replaced by the operand.");
            }
            final String firstOperandsExpression = new DynamodbFilterExpressionFactory()
                    .buildFilterExpression(operands.get(0), this.valueListBuilder);
            final LogicalOperator.Operator operator = logicalOperator.getOperator();
            this.filterExpression = firstOperandsExpression + " " + getComparisionOperatorsExpression(operator) + " "
                    + getSecondOperandsExpression(operands, operator);
        }

        private String getSecondOperandsExpression(final List<QueryPredicate<DynamodbNodeVisitor>> operands,
                final LogicalOperator.Operator operator) {
            if (operands.size() > 2) {
                final List<QueryPredicate<DynamodbNodeVisitor>> remainingOperands = operands.subList(1,
                        operands.size());
                final LogicalOperator<DynamodbNodeVisitor> logicalOperatorForRemaining = new LogicalOperator<>(
                        remainingOperands, operator);
                return "(" + new DynamodbFilterExpressionFactory().buildFilterExpression(logicalOperatorForRemaining,
                        this.valueListBuilder) + ")";
            } else {
                return new DynamodbFilterExpressionFactory().buildFilterExpression(operands.get(1),
                        this.valueListBuilder);
            }
        }

        private String getComparisionOperatorsExpression(final LogicalOperator.Operator operator) {
            if (operator == LogicalOperator.Operator.AND) {
                return "and";
            } else {
                return "or";
            }
        }

        @Override
        public void visit(final NoPredicate<DynamodbNodeVisitor> noPredicate) {
            this.filterExpression = "";
        }

        public String getFilterExpression() {
            return this.filterExpression;
        }
    }
}
