package com.exasol.adapter.document.documentfetcher.dynamodb;

import java.util.Optional;
import java.util.Set;

import com.exasol.adapter.document.dynamodbmetadata.DynamodbIndex;
import com.exasol.adapter.document.querypredicate.ColumnLiteralComparisonPredicate;
import com.exasol.adapter.document.querypredicate.LogicalOperator;
import com.exasol.adapter.document.querypredicate.QueryPredicate;
import com.exasol.adapter.document.querypredicate.normalizer.DnfOr;

/**
 * This class represents the selection predicates of a DynamoDB query operation.
 * 
 * The selection is split up in a index-selection and an non-index-selection. The two selections result in the original
 * selection, when they are combined with an AND.
 *
 * The index selection is again split into a partition key condition and a sort key condition.
 */
class QueryOperationSelection {
    private final ColumnLiteralComparisonPredicate partitionKeyCondition;
    private final Optional<ColumnLiteralComparisonPredicate> sortKeyCondition;
    private final DnfOr nonIndexSelection;
    private final DynamodbIndex index;

    /**
     * Create an instance of {@link QueryOperationSelection}.
     * 
     * @param partitionKeyCondition selection on the partition key
     * @param sortKeyCondition      selection on the sort key
     * @param nonIndexSelection     selection on all non index columns
     * @param index                 index that is used for the query
     */
    QueryOperationSelection(final ColumnLiteralComparisonPredicate partitionKeyCondition,
            final Optional<ColumnLiteralComparisonPredicate> sortKeyCondition, final DnfOr nonIndexSelection,
            final DynamodbIndex index) {
        this.partitionKeyCondition = partitionKeyCondition;
        this.sortKeyCondition = sortKeyCondition;
        this.nonIndexSelection = nonIndexSelection;
        this.index = index;
    }

    /**
     * Get the sort key condition.
     * 
     * @return sort key condition.
     */
    public Optional<ColumnLiteralComparisonPredicate> getSortKeyCondition() {
        return this.sortKeyCondition;
    }

    /**
     * Get the selection predicate for the non index columns.
     *
     * @return non-index selection
     */
    public DnfOr getNonIndexSelection() {
        return this.nonIndexSelection;
    }

    /**
     * Get the selection predicate for the index selection.
     * 
     * @return index-selection predicate
     */
    public QueryPredicate getIndexSelectionAsQueryPredicate() {
        if (this.sortKeyCondition.isEmpty()) {
            return this.partitionKeyCondition;
        } else {
            return new LogicalOperator(Set.of(this.partitionKeyCondition, this.sortKeyCondition.get()),
                    LogicalOperator.Operator.AND);
        }
    }

    /**
     * Get the index used for this query
     * 
     * @return {@link DynamodbIndex}
     */
    public DynamodbIndex getIndex() {
        return this.index;
    }
}
