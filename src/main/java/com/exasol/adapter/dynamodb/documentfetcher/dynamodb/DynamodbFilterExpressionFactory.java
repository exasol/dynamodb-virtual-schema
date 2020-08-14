package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import java.util.HashSet;
import java.util.Set;

import com.exasol.adapter.dynamodb.documentnode.DocumentValue;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.dynamodb.literalconverter.NotLiteralException;
import com.exasol.adapter.dynamodb.literalconverter.dynamodb.SqlLiteralToDynamodbValueConverter;
import com.exasol.adapter.dynamodb.mapping.ColumnMapping;
import com.exasol.adapter.dynamodb.mapping.PropertyToColumnMapping;
import com.exasol.adapter.dynamodb.querypredicate.*;

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
    public String buildFilterExpression(final QueryPredicate predicate) {
        final Visitor visitor = new Visitor(this.namePlaceholderMapBuilder, this.valuePlaceholderMapBuilder);
        predicate.accept(visitor);
        return visitor.getFilterExpression();
    }

    private static class Visitor implements QueryPredicateVisitor {
        private final DynamodbAttributeNamePlaceholderMapBuilder namePlaceholderMapBuilder;
        private final DynamodbAttributeValuePlaceholderMapBuilder valuePlaceholderMapBuilder;
        private String filterExpression;

        private Visitor(final DynamodbAttributeNamePlaceholderMapBuilder namePlaceholderMapBuilder,
                final DynamodbAttributeValuePlaceholderMapBuilder valuePlaceholderMapBuilder) {
            this.namePlaceholderMapBuilder = namePlaceholderMapBuilder;
            this.valuePlaceholderMapBuilder = valuePlaceholderMapBuilder;
        }

        @Override
        public void visit(final ComparisonPredicate comparisonPredicate) {
            final ComparisonPredicateConverter visitor = new ComparisonPredicateConverter(
                    this.namePlaceholderMapBuilder, this.valuePlaceholderMapBuilder);
            comparisonPredicate.accept(visitor);
            this.filterExpression = visitor.filterExpression;
        }

        @Override
        public void visit(final LogicalOperator logicalOperator) {
            final Set<QueryPredicate> operands = logicalOperator.getOperands();
            if (operands.isEmpty()) {
                throw new IllegalArgumentException(
                        "Empty logic expressions must be removed before converting to FilterExpression.");
            } else if (operands.size() == 1) {
                this.filterExpression = convertPredicateRecursively(operands.iterator().next());
            } else {
                final QueryPredicate firstPredicate = operands.iterator().next();
                final Set<QueryPredicate> remainingOperands = getRemainingOperands(operands, firstPredicate);
                final String firstOperandsExpression = convertPredicateRecursively(firstPredicate);
                final LogicalOperator.Operator operator = logicalOperator.getOperator();
                this.filterExpression = "(" + firstOperandsExpression + " "
                        + getComparisionOperatorsExpression(operator) + " "
                        + getRemainingOperandsExpression(remainingOperands, operator) + ")";
            }
        }

        private Set<QueryPredicate> getRemainingOperands(final Set<QueryPredicate> operands,
                final QueryPredicate firstPredicate) {
            final HashSet<QueryPredicate> remainingPredicates = new HashSet<>(operands);
            remainingPredicates.remove(firstPredicate);
            return remainingPredicates;
        }

        private String getRemainingOperandsExpression(final Set<QueryPredicate> remainingOperands,
                final LogicalOperator.Operator operator) {
            final LogicalOperator logicalOperatorForRemaining = new LogicalOperator(remainingOperands, operator);
            return "(" + convertPredicateRecursively(logicalOperatorForRemaining) + ")";
        }

        private String getComparisionOperatorsExpression(final LogicalOperator.Operator operator) {
            if (operator == LogicalOperator.Operator.AND) {
                return "and";
            } else {
                return "or";
            }
        }

        @Override
        public void visit(final NoPredicate noPredicate) {
            this.filterExpression = "";
        }

        @Override
        public void visit(final NotPredicate notPredicate) {
            this.filterExpression = "NOT (" + convertPredicateRecursively(notPredicate.getPredicate()) + ")";
        }

        private String convertPredicateRecursively(final QueryPredicate predicate) {
            return new DynamodbFilterExpressionFactory(this.namePlaceholderMapBuilder, this.valuePlaceholderMapBuilder)
                    .buildFilterExpression(predicate);
        }

        public String getFilterExpression() {
            return this.filterExpression;
        }
    }

    private static class ComparisonPredicateConverter implements ComparisonPredicateVisitor {
        private final DynamodbAttributeNamePlaceholderMapBuilder namePlaceholderMapBuilder;
        private final DynamodbAttributeValuePlaceholderMapBuilder valuePlaceholderMapBuilder;
        private String filterExpression;

        private ComparisonPredicateConverter(final DynamodbAttributeNamePlaceholderMapBuilder namePlaceholderMapBuilder,
                final DynamodbAttributeValuePlaceholderMapBuilder valuePlaceholderMapBuilder) {
            this.namePlaceholderMapBuilder = namePlaceholderMapBuilder;
            this.valuePlaceholderMapBuilder = valuePlaceholderMapBuilder;
        }

        @Override
        public void visit(final ColumnLiteralComparisonPredicate columnLiteralComparisonPredicate) {
            final ColumnMapping column = columnLiteralComparisonPredicate.getColumn();
            if (column instanceof PropertyToColumnMapping) {
                final PropertyToColumnMapping columnMapping = (PropertyToColumnMapping) column;
                final DocumentPathExpression columnsPath = columnMapping.getPathToSourceProperty();
                final String columnPathExpression = DocumentPathToDynamodbExpressionConverter.getInstance()
                        .convert(columnsPath, this.namePlaceholderMapBuilder);
                final DocumentValue<DynamodbNodeVisitor> literal = getLiteral(columnLiteralComparisonPredicate);
                final String valuePlaceholder = this.valuePlaceholderMapBuilder.addValue(literal);
                this.filterExpression = columnPathExpression + " "
                        + convertComparisonOperator(columnLiteralComparisonPredicate.getOperator()) + " "
                        + valuePlaceholder;
            } else {
                throw new UnsupportedOperationException("This column has no corresponding DynamoDB column. "
                        + "Hence it can't be part of a filter expression.");
            }
        }

        private DocumentValue<DynamodbNodeVisitor> getLiteral(
                final ColumnLiteralComparisonPredicate columnLiteralComparisonPredicate) {
            try {
                return new SqlLiteralToDynamodbValueConverter().convert(columnLiteralComparisonPredicate.getLiteral());
            } catch (final NotLiteralException exception) {
                throw new UnsupportedOperationException("Invalid comparison to a non literal.");
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
