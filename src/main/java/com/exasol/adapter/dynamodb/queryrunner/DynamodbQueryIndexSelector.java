package com.exasol.adapter.dynamodb.queryrunner;

import java.util.List;
import java.util.stream.Collectors;

import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.dynamodbmetadata.AbstractDynamodbIndex;
import com.exasol.adapter.dynamodb.remotetablequery.QueryPredicate;

/**
 * This class picks the {@link AbstractDynamodbIndex} that's keys are most restricted in a given query..
 */
public class DynamodbQueryIndexSelector {

    /**
     * Searches for the index that's keys are most selective restricted in the given query.
     *
     * First indexes, that's partition key is not in an equality comparision are sorted out, as not {@code QUERY}
     * operation is possible without a equality constraint on the partition key anyway.
     * 
     * Next the index that's search key is most selective compared in the query is returned.
     *
     * @param selection        selection to analyse
     * @param availableIndexes list of possible DynamoDB indexes
     * @return most restricted index or null if non did match the criteria
     */
    public AbstractDynamodbIndex findMostRestrictedIndex(final QueryPredicate<DynamodbNodeVisitor> selection,
            final List<AbstractDynamodbIndex> availableIndexes) {
        final List<AbstractDynamodbIndex> suitableKeys = extractSuitableIndexes(selection, availableIndexes);
        return findMostRestrictedIndexBySortKey(selection, suitableKeys);
    }

    private List<AbstractDynamodbIndex> extractSuitableIndexes(final QueryPredicate<DynamodbNodeVisitor> selection,
            final List<AbstractDynamodbIndex> availableKeys) {
        return availableKeys.stream().filter(key -> isIndexesPrimaryKeyRestrictedInEqualitySelection(key, selection))
                .collect(Collectors.toList());
    }

    private boolean isIndexesPrimaryKeyRestrictedInEqualitySelection(final AbstractDynamodbIndex index,
            final QueryPredicate<DynamodbNodeVisitor> selection) {
        try {
            final QueryPredicate<DynamodbNodeVisitor> partitionKeySelection = new DynamodbQuerySelectionFilter()
                    .filter(selection, List.of(index.getPartitionKey()));
            final int partitionKeyRating = new DynamodbQuerySelectionRater().rate(partitionKeySelection);
            return partitionKeyRating == DynamodbQuerySelectionRater.RATING_EQUALITY;
        } catch (final DynamodbQuerySelectionFilterException exception) {
            return false;
        }
    }

    private AbstractDynamodbIndex findMostRestrictedIndexBySortKey(final QueryPredicate<DynamodbNodeVisitor> selection,
            final List<AbstractDynamodbIndex> suitableKeys) {
        int bestKeysRating = -1;
        AbstractDynamodbIndex bestKey = null;
        for (final AbstractDynamodbIndex index : suitableKeys) {
            try {
                final int keysRating = rateSortKeySelectivity(selection, index);
                if (keysRating > bestKeysRating) {
                    bestKeysRating = keysRating;
                    bestKey = index;
                }
            } catch (final DynamodbQuerySelectionFilterException exception) {
                // we just ignore this index if it does not fit.
            }
        }
        return bestKey;
    }

    private int rateSortKeySelectivity(final QueryPredicate<DynamodbNodeVisitor> selection,
            final AbstractDynamodbIndex index) {
        if (!index.hasSortKey()) {
            return DynamodbQuerySelectionRater.RATING_NO_SELECTIVITY; // as this index has no sort key it does not have
                                                                      // any selectivity
        }
        final QueryPredicate<DynamodbNodeVisitor> selectionOnSortKey = new DynamodbQuerySelectionFilter()
                .filter(selection, List.of(index.getSortKey()));
        return new DynamodbQuerySelectionRater().rate(selectionOnSortKey);
    }
}
