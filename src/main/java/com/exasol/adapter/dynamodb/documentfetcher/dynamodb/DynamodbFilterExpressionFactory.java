package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import java.util.List;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeToAttributeValueConverter;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.dynamodb.mapping.ColumnMapping;
import com.exasol.adapter.dynamodb.mapping.PropertyToColumnMapping;
import com.exasol.adapter.dynamodb.remotetablequery.*;

/**
 * This class builds a DynamoDB filter expression for a given selection.
 */
public class DynamodbFilterExpressionFactory {
    private final DynamodbAttributeNamePlaceholderMapBuilder namePlaceholderMapBuilder;
    private final DynamodbAttributeValuePlaceholderMapBuilder valuePlaceholderMapBuilder;

    /**
     * Create an instance of {@link DynamodbFilterExpressionFactory}.
     * 
     * @param namePlaceholderMapBuilder  builder that takes attribute names and gives placeholders for them
     * @param valuePlaceholderMapBuilder builder that takes literals and gives placeholders for them.
     */
    public DynamodbFilterExpressionFactory(final DynamodbAttributeNamePlaceholderMapBuilder namePlaceholderMapBuilder,
            final DynamodbAttributeValuePlaceholderMapBuilder valuePlaceholderMapBuilder) {
        this.namePlaceholderMapBuilder = namePlaceholderMapBuilder;
        this.valuePlaceholderMapBuilder = valuePlaceholderMapBuilder;
    }

    /**
     * Build a DynamoDB filter expression for the given predicate.
     * 
     * @param predicate predicate that will be converted into a filter expression
     * @return DynamoDB filter expression
     */
    public String buildFilterExpression(final QueryPredicate<DynamodbNodeVisitor> predicate) {
        final Visitor visitor = new Visitor(this.namePlaceholderMapBuilder, this.valuePlaceholderMapBuilder);
        predicate.accept(visitor);
        return visitor.getFilterExpression();
    }

    private static class Visitor implements QueryPredicateVisitor<DynamodbNodeVisitor> {
        private final DynamodbAttributeNamePlaceholderMapBuilder namePlaceholderMapBuilder;
        private final DynamodbAttributeValuePlaceholderMapBuilder valuePlaceholderMapBuilder;
        private String filterExpression;

        private Visitor(final DynamodbAttributeNamePlaceholderMapBuilder namePlaceholderMapBuilder,
                final DynamodbAttributeValuePlaceholderMapBuilder valuePlaceholderMapBuilder) {
            this.namePlaceholderMapBuilder = namePlaceholderMapBuilder;
            this.valuePlaceholderMapBuilder = valuePlaceholderMapBuilder;
        }

        @Override
        public void visit(final ComparisonPredicate<DynamodbNodeVisitor> comparisonPredicate) {
            final ComparisonPredicateConverter visitor = new ComparisonPredicateConverter(
                    this.namePlaceholderMapBuilder, this.valuePlaceholderMapBuilder);
            comparisonPredicate.accept(visitor);
            this.filterExpression = visitor.filterExpression;
        }

        @Override
        public void visit(final LogicalOperator<DynamodbNodeVisitor> logicalOperator) {
            final List<QueryPredicate<DynamodbNodeVisitor>> operands = logicalOperator.getOperands();
            if (operands.size() < 2) {
                throw new IllegalArgumentException(
                        "Logic expressions with only one operand must be replaced by the operand.");
            }
            final String firstOperandsExpression = callRecursive(operands.get(0));
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
                return "(" + callRecursive(logicalOperatorForRemaining) + ")";
            } else {
                return callRecursive(operands.get(1));
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

        @Override
        public void visit(final NotPredicate<DynamodbNodeVisitor> notPredicate) {
            this.filterExpression = "NOT (" + callRecursive(notPredicate.getPredicate()) + ")";
        }

        private String callRecursive(final QueryPredicate<DynamodbNodeVisitor> predicate) {
            return new DynamodbFilterExpressionFactory(this.namePlaceholderMapBuilder, this.valuePlaceholderMapBuilder)
                    .buildFilterExpression(predicate);
        }

        public String getFilterExpression() {
            return this.filterExpression;
        }
    }

    private static class ComparisonPredicateConverter implements ComparisonPredicateVisitor<DynamodbNodeVisitor> {
        private final DynamodbAttributeNamePlaceholderMapBuilder namePlaceholderMapBuilder;
        private final DynamodbAttributeValuePlaceholderMapBuilder valuePlaceholderMapBuilder;
        private String filterExpression;

        private ComparisonPredicateConverter(final DynamodbAttributeNamePlaceholderMapBuilder namePlaceholderMapBuilder,
                final DynamodbAttributeValuePlaceholderMapBuilder valuePlaceholderMapBuilder) {
            this.namePlaceholderMapBuilder = namePlaceholderMapBuilder;
            this.valuePlaceholderMapBuilder = valuePlaceholderMapBuilder;
        }

        @Override
        public void visit(
                final ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> columnLiteralComparisonPredicate) {
            final ColumnMapping column = columnLiteralComparisonPredicate.getColumn();
            if (column instanceof PropertyToColumnMapping) {
                final PropertyToColumnMapping columnMapping = (PropertyToColumnMapping) column;
                final DocumentPathExpression columnsPath = columnMapping.getPathToSourceProperty();
                final String columnPathExpression = new DocumentPathToDynamodbExpressionConverter()
                        .convert(columnsPath);
                final String namePlaceholder = this.namePlaceholderMapBuilder.addValue(columnPathExpression);
                final AttributeValue attributeValue = new DynamodbNodeToAttributeValueConverter()
                        .convertToAttributeValue(columnLiteralComparisonPredicate.getLiteral());
                final String valuePlaceholder = this.valuePlaceholderMapBuilder.addValue(attributeValue);
                this.filterExpression = namePlaceholder + " "
                        + convertComparisonOperator(columnLiteralComparisonPredicate.getOperator()) + " "
                        + valuePlaceholder;
            } else {
                throw new UnsupportedOperationException("This column has no corresponding DynamoDB column. "
                        + "Hence it can't be part of a filter expression.");
            }
        }

        private String convertComparisonOperator(final AbstractComparisonPredicate.Operator operator) {
            switch (operator) {
            case EQUAL:
                return "=";
            case NOT_EQUAL:
                return "<>";
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
    }
}
