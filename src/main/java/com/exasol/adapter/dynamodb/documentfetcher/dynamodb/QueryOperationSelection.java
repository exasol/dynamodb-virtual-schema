package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import java.util.List;
import java.util.Optional;

import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbIndex;
import com.exasol.adapter.dynamodb.remotetablequery.ColumnLiteralComparisonPredicate;
import com.exasol.adapter.dynamodb.remotetablequery.LogicalOperator;
import com.exasol.adapter.dynamodb.remotetablequery.QueryPredicate;
import com.exasol.adapter.dynamodb.remotetablequery.normalizer.DnfOr;

/**
 * This class represents the selection predicates of a DynamoDB query operation.
 * 
 * The selection is split up in a index-selection and an non-index-selection. The two selections result in the original
 * selection, when they are combined with an AND.
 *
 * The index selection is again split into a partition key condition and a sort key condition.
 */
class QueryOperationSelection {
    private final ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> partitionKeyCondition;
    private final Optional<ColumnLiteralComparisonPredicate<DynamodbNodeVisitor>> sortKeyCondition;
    private final DnfOr<DynamodbNodeVisitor> nonIndexSelection;
    private final DynamodbIndex index;

    /**
     * Create an instance of {@link QueryOperationSelection}.
     * 
     * @param partitionKeyCondition selection on the partition key
     * @param sortKeyCondition      selection on the sort key
     * @param nonIndexSelection     selection on all non index columns
     * @param index                 index that is used for the query
     */
    QueryOperationSelection(final ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> partitionKeyCondition,
            final Optional<ColumnLiteralComparisonPredicate<DynamodbNodeVisitor>> sortKeyCondition,
            final DnfOr<DynamodbNodeVisitor> nonIndexSelection, final DynamodbIndex index) {
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
    public Optional<ColumnLiteralComparisonPredicate<DynamodbNodeVisitor>> getSortKeyCondition() {
        return this.sortKeyCondition;
    }

    /**
     * Get the selection predicate for the non index columns.
     *
     * @return non-index selection
     */
    public DnfOr<DynamodbNodeVisitor> getNonIndexSelection() {
        return this.nonIndexSelection;
    }

    /**
     * Get the selection predicate for the index selection.
     * 
     * @return index-selection predicate
     */
    public QueryPredicate<DynamodbNodeVisitor> getIndexSelectionAsQueryPredicate() {
        if (this.sortKeyCondition.isEmpty()) {
            return this.partitionKeyCondition;
        } else {
            return new LogicalOperator<>(List.of(this.partitionKeyCondition, this.sortKeyCondition.get()),
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
