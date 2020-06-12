package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbIndex;
import com.exasol.adapter.dynamodb.mapping.PropertyToColumnMapping;
import com.exasol.adapter.dynamodb.querypredicate.AbstractComparisonPredicate;
import com.exasol.adapter.dynamodb.querypredicate.ColumnLiteralComparisonPredicate;
import com.exasol.adapter.dynamodb.querypredicate.ComparisonPredicate;
import com.exasol.adapter.dynamodb.querypredicate.normalizer.DnfAnd;
import com.exasol.adapter.dynamodb.querypredicate.normalizer.DnfComparison;
import com.exasol.adapter.dynamodb.querypredicate.normalizer.DnfOr;

/**
 * This Factory builds {@link QueryOperationSelection}s for given selection predicate and DynamoDB index.
 */
class QueryOperationSelectionFactory {

    /**
     * Build {@link QueryOperationSelection}s for given selection predicate and DynamoDB index.
     * 
     * @param dnfOr selection predicate
     * @param index DynamoDB index for the query operation
     * @return {@link QueryOperationSelection}
     */
    public QueryOperationSelection build(final DnfOr<DynamodbNodeVisitor> dnfOr, final DynamodbIndex index) {
        final ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> partitionKeyCondition = extractPartitionKeyCondition(
                dnfOr, index);
        final Optional<ColumnLiteralComparisonPredicate<DynamodbNodeVisitor>> sortKeyCondition = extractSortKeyCondition(
                dnfOr, index);
        final DnfOr<DynamodbNodeVisitor> nonIndexSelection = extractNonIndexSelection(dnfOr, index);
        return new QueryOperationSelection(partitionKeyCondition, sortKeyCondition, nonIndexSelection, index);
    }

    private DnfOr<DynamodbNodeVisitor> extractNonIndexSelection(final DnfOr<DynamodbNodeVisitor> dnfOr,
            final DynamodbIndex index) {
        final Set<DnfAnd<DynamodbNodeVisitor>> operands = dnfOr.getOperands().stream()
                .map(and -> getAndOfNonIndexComparisons(index, and)).collect(Collectors.toSet());
        return new DnfOr<>(operands);
    }

    private DnfAnd<DynamodbNodeVisitor> getAndOfNonIndexComparisons(final DynamodbIndex index,
            final DnfAnd<DynamodbNodeVisitor> and) {
        return new DnfAnd<>(and.getOperands().stream().filter(
                comparison -> !isComparisonOnProperty(comparison.getComparisonPredicate(), index.getPartitionKey())
                        && !isComparisonOnProperty(comparison.getComparisonPredicate(), index.getSortKey()))
                .collect(Collectors.toSet()));
    }

    private ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> extractPartitionKeyCondition(
            final DnfOr<DynamodbNodeVisitor> dnfOr, final DynamodbIndex index) {
        final Set<ColumnLiteralComparisonPredicate<DynamodbNodeVisitor>> partitionKeyConditions = dnfOr.getOperands()
                .stream().map(dnfAnd -> extractPartitionKeyConditionsFromAnd(index, dnfAnd))
                .filter(this::hasOnlyOnePartitionKeyCondition).filter(this::abortIfOneAndHasNoPartitionKey)
                .map(set -> set.iterator().next()).collect(Collectors.toSet());
        if (partitionKeyConditions.size() > 1) {
            throw new PlanDoesNotFitException("This query specifies more than one partition key. "
                    + "This could be solved by multiple query operations, but is not implemented yet.");
        }
        if (partitionKeyConditions.isEmpty()) {
            throw new PlanDoesNotFitException(
                    "This query does not specify a partition key. Use a SCAN operation instead.");
        }
        return partitionKeyConditions.iterator().next();
    }

    private Optional<ColumnLiteralComparisonPredicate<DynamodbNodeVisitor>> extractSortKeyCondition(
            final DnfOr<DynamodbNodeVisitor> dnfOr, final DynamodbIndex index) {
        final Set<Optional<ColumnLiteralComparisonPredicate<DynamodbNodeVisitor>>> andsSortKeyConditions = dnfOr
                .getOperands().stream().map(dnfAnd -> extractSortKeyConditionsFromAnd(index, dnfAnd))
                .collect(Collectors.toSet());
        if (andsSortKeyConditions.isEmpty()) {
            return Optional.empty();
        } else if (andsSortKeyConditions.size() > 1) {
            // TODO optimization: split query
            throw new PlanDoesNotFitException(
                    "This is not a Query operation as different sort key conditions are used.");
        } else {
            return andsSortKeyConditions.iterator().next();
        }
    }

    private boolean abortIfOneAndHasNoPartitionKey(
            final Set<ColumnLiteralComparisonPredicate<DynamodbNodeVisitor>> andsPartitionKeys) {
        if (andsPartitionKeys.isEmpty()) {
            throw new PlanDoesNotFitException(
                    "One ore mote predicates does not specify a partiton key. Therefore this Query requires a SCAN operation.");
        }
        return true;
    }

    /**
     * Extract partition key values from and AND in the DNF.
     *
     * example: {@code dnfAnd: {@code isbn = 123 AND price > 10} index: (partitionKey = isbn) --> Set(isbn = 23) }
     *
     * @param index  DynamoDB index defining the partition key name
     * @param dnfAnd AND predicate from the DNF
     * @return Set of conditions that involve the partition key.
     */
    private Set<ColumnLiteralComparisonPredicate<DynamodbNodeVisitor>> extractPartitionKeyConditionsFromAnd(
            final DynamodbIndex index, final DnfAnd<DynamodbNodeVisitor> dnfAnd) {
        return dnfAnd.getOperands().stream()
                .filter(comparisonPredicate -> isComparisonOnProperty(comparisonPredicate.getComparisonPredicate(),
                        index.getPartitionKey()))
                .filter(this::abortIfNotEqualityComparison).map(DnfComparison::getComparisonPredicate)
                .map(this::castToColumnLiteralComparisonWithAbortIfNotPossible).collect(Collectors.toSet());
    }

    private Optional<ColumnLiteralComparisonPredicate<DynamodbNodeVisitor>> extractSortKeyConditionsFromAnd(
            final DynamodbIndex index, final DnfAnd<DynamodbNodeVisitor> dnfAnd) {
        final Set<ColumnLiteralComparisonPredicate<DynamodbNodeVisitor>> conditions = dnfAnd.getOperands().stream()
                .map(this::extractComparisonPredicate)
                .filter(comparisonPredicate -> isComparisonOnProperty(comparisonPredicate, index.getSortKey()))
                .map(this::castToColumnLiteralComparisonWithAbortIfNotPossible).collect(Collectors.toSet());
        if (conditions.size() > 1) {
            // TODO optimization: try to merge conditions
            throw new PlanDoesNotFitException("Different sort key conditions are not supported in a single AND.");
        } else if (conditions.size() == 1) {
            return Optional.of(conditions.iterator().next());
        } else {
            return Optional.empty();
        }
    }

    private ComparisonPredicate<DynamodbNodeVisitor> extractComparisonPredicate(
            final DnfComparison<DynamodbNodeVisitor> comparison) {
        if (comparison.isNegated()) {
            final ComparisonPredicate<DynamodbNodeVisitor> negated = comparison.getComparisonPredicate().negate();
            if (negated.getOperator().equals(AbstractComparisonPredicate.Operator.NOT_EQUAL)) {
                throw new PlanDoesNotFitException("DynamoDB does not support the <> operator for key conditions.");
            }
            return negated;
        } else {
            return comparison.getComparisonPredicate();
        }
    }

    /**
     * If a AND specifies more that values for an equality comparison for the partition key, the AND is equal to false
     * and so can be left out as X OR false --> X. e.g. isbn = 123 AND isbn = 456 --> false
     *
     * As the input value is a set it only contains distinct values. That means, if there are more then two input
     * values, there are different comparisons on the partition key.
     */
    private boolean hasOnlyOnePartitionKeyCondition(
            final Set<ColumnLiteralComparisonPredicate<DynamodbNodeVisitor>> partitionKeyValues) {
        return partitionKeyValues.size() <= 1;
    }

    private boolean isComparisonOnProperty(final ComparisonPredicate<DynamodbNodeVisitor> comparison,
            final String propertyName) {
        final DocumentPathExpression keyPath = new DocumentPathExpression.Builder().addObjectLookup(propertyName)
                .build();
        return comparison.getComparedColumns().stream().filter(column -> column instanceof PropertyToColumnMapping)
                .map(column -> (PropertyToColumnMapping) column)
                .anyMatch(column -> column.getPathToSourceProperty().equals(keyPath));
    }

    private boolean abortIfNotEqualityComparison(final DnfComparison<DynamodbNodeVisitor> comparison) {
        if (comparison.getComparisonPredicate().getOperator().equals(AbstractComparisonPredicate.Operator.EQUAL)
                && !comparison.isNegated()) {
            return true;
        } else {
            throw new PlanDoesNotFitException("Dynamodb does only support equality comparisons on the primary key.");
        }
    }

    private ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> castToColumnLiteralComparisonWithAbortIfNotPossible(
            final ComparisonPredicate<DynamodbNodeVisitor> comparisonPredicate) {
        if (comparisonPredicate instanceof ColumnLiteralComparisonPredicate) {
            return (ColumnLiteralComparisonPredicate<DynamodbNodeVisitor>) comparisonPredicate;
        } else {
            throw new PlanDoesNotFitException("Dynmaodb does only support comparisons to a literal for key columns.");
        }
    }
}
