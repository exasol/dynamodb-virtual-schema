package com.exasol.adapter.document.documentfetcher.dynamodb;

import com.exasol.adapter.document.documentpath.DocumentPathExpression;
import com.exasol.adapter.document.dynamodbmetadata.DynamodbIndex;
import com.exasol.adapter.document.mapping.PropertyToColumnMapping;
import com.exasol.adapter.document.querypredicate.AbstractComparisonPredicate.Operator;
import com.exasol.adapter.document.querypredicate.ColumnLiteralComparisonPredicate;
import com.exasol.adapter.document.querypredicate.ComparisonPredicate;
import com.exasol.adapter.document.querypredicate.normalizer.*;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.exasol.adapter.document.querypredicate.AbstractComparisonPredicate.Operator.*;

/**
 * This Factory builds {@link QueryOperationSelection}s for given selection predicate and DynamoDB index.
 */
class QueryOperationSelectionFactory {
    private static final ComparisonNegator NEGATOR = new ComparisonNegator();
    private static final Set<Operator> SUPPORTED_OPERATORS_FOR_KEY_COLUMNS = Set.of(EQUAL, LESS, LESS_EQUAL, GREATER,
            GREATER_EQUAL);

    /**
     * Build {@link QueryOperationSelection}s for given selection predicate and DynamoDB index.
     *
     * @param dnfOr selection predicate
     * @param index DynamoDB index for the query operation
     * @return {@link QueryOperationSelection}
     */
    public QueryOperationSelection build(final DnfOr dnfOr, final DynamodbIndex index) {
        final ColumnLiteralComparisonPredicate partitionKeyCondition = extractPartitionKeyCondition(dnfOr, index);
        final Optional<ColumnLiteralComparisonPredicate> sortKeyCondition = extractSortKeyCondition(dnfOr, index);
        final DnfOr nonIndexSelection = extractNonIndexSelection(dnfOr, index);
        return new QueryOperationSelection(partitionKeyCondition, sortKeyCondition, nonIndexSelection, index);
    }

    private DnfAnd getAndOfNonIndexComparisons(final DynamodbIndex index, final DnfAnd and) {
        return new DnfAnd(and.getOperands().stream().filter(
                comparison -> !isComparisonOnProperty(comparison.getComparisonPredicate(), index.getPartitionKey())
                        && !isComparisonOnProperty(comparison.getComparisonPredicate(), index.getSortKey()))
                .collect(Collectors.toSet()));
    }

    private DnfOr extractNonIndexSelection(final DnfOr dnfOr, final DynamodbIndex index) {
        final Set<DnfAnd> operands = dnfOr.getOperands().stream().map(and -> getAndOfNonIndexComparisons(index, and))
                .collect(Collectors.toSet());
        return new DnfOr(operands);
    }

    private ColumnLiteralComparisonPredicate extractPartitionKeyCondition(final DnfOr dnfOr,
                                                                          final DynamodbIndex index) {
        final Set<ColumnLiteralComparisonPredicate> partitionKeyConditions = dnfOr.getOperands().stream()
                .map(dnfAnd -> extractPartitionKeyConditionsFromAnd(index, dnfAnd))
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

    private Optional<ColumnLiteralComparisonPredicate> extractSortKeyCondition(final DnfOr dnfOr,
                                                                               final DynamodbIndex index) {
        final Set<Optional<ColumnLiteralComparisonPredicate>> andsSortKeyConditions = dnfOr.getOperands().stream()
                .map(dnfAnd -> extractSortKeyConditionsFromAnd(index, dnfAnd)).collect(Collectors.toSet());
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

    /**
     * Extract partition key values from and AND in the DNF.
     * <p>
     * example: {@code dnfAnd: {@code isbn = 123 AND price > 10} index: (partitionKey = isbn) --> Set(isbn = 23) }
     *
     * @param index  DynamoDB index defining the partition key name
     * @param dnfAnd AND predicate from the DNF
     * @return Set of conditions that involve the partition key.
     */
    private Set<ColumnLiteralComparisonPredicate> extractPartitionKeyConditionsFromAnd(final DynamodbIndex index,
                                                                                       final DnfAnd dnfAnd) {
        return dnfAnd.getOperands().stream()
                .filter(comparisonPredicate -> isComparisonOnProperty(comparisonPredicate.getComparisonPredicate(),
                        index.getPartitionKey()))
                .filter(this::abortIfNotEqualityComparison).map(DnfComparison::getComparisonPredicate)
                .map(this::castToColumnLiteralComparisonWithAbortIfNotPossible).collect(Collectors.toSet());
    }

    private Optional<ColumnLiteralComparisonPredicate> extractSortKeyConditionsFromAnd(final DynamodbIndex index,
                                                                                       final DnfAnd dnfAnd) {
        final Set<ColumnLiteralComparisonPredicate> conditions = dnfAnd.getOperands().stream()
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

    private boolean abortIfOneAndHasNoPartitionKey(final Set<ColumnLiteralComparisonPredicate> andsPartitionKeys) {
        if (andsPartitionKeys.isEmpty()) {
            throw new PlanDoesNotFitException(
                    "One or more predicates does not specify a partiton key. Therefore this Query requires a SCAN operation.");
        }
        return true;
    }

    private ComparisonPredicate extractComparisonPredicate(final DnfComparison comparison) {
        if (comparison.isNegated()) {
            final ComparisonPredicate negated = NEGATOR.negate(comparison.getComparisonPredicate());
            if (!SUPPORTED_OPERATORS_FOR_KEY_COLUMNS.contains(negated.getOperator())) {
                throw new PlanDoesNotFitException(
                        "DynamoDB does not support the " + negated.getOperator() + " operator for key conditions.");
            }
            return negated;
        } else {
            return comparison.getComparisonPredicate();
        }
    }

    /**
     * If a AND specifies more that values for an equality comparison for the partition key, the AND is equal to false
     * and so can be left out as X OR false --> X. e.g. isbn = 123 AND isbn = 456 --> false
     * <p>
     * As the input value is a set it only contains distinct values. That means, if there are more then two input
     * values, there are different comparisons on the partition key.
     */
    private boolean hasOnlyOnePartitionKeyCondition(final Set<ColumnLiteralComparisonPredicate> partitionKeyValues) {
        return partitionKeyValues.size() <= 1;
    }

    private boolean isComparisonOnProperty(final ComparisonPredicate comparison, final String propertyName) {
        final DocumentPathExpression keyPath = DocumentPathExpression.builder().addObjectLookup(propertyName).build();
        return comparison.getComparedColumns().stream().filter(column -> column instanceof PropertyToColumnMapping)
                .map(PropertyToColumnMapping.class::cast)
                .anyMatch(column -> column.getPathToSourceProperty().equals(keyPath));
    }

    private boolean abortIfNotEqualityComparison(final DnfComparison comparison) {
        if (comparison.getComparisonPredicate().getOperator().equals(EQUAL) && !comparison.isNegated()) {
            return true;
        } else {
            throw new PlanDoesNotFitException("Dynamodb does only support equality comparisons on the primary key.");
        }
    }

    private ColumnLiteralComparisonPredicate castToColumnLiteralComparisonWithAbortIfNotPossible(
            final ComparisonPredicate comparisonPredicate) {
        if (comparisonPredicate instanceof ColumnLiteralComparisonPredicate) {
            return (ColumnLiteralComparisonPredicate) comparisonPredicate;
        } else {
            throw new PlanDoesNotFitException("Dynmaodb does only support comparisons to a literal for key columns.");
        }
    }
}
