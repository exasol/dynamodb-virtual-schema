package com.exasol.adapter.dynamodb.queryrunner;

import java.util.List;
import java.util.stream.Collectors;

import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbKey;
import com.exasol.adapter.dynamodb.remotetablequery.QueryPredicate;

/**
 * This class picks the best {@link DynamodbKey} for a {@code QUERY} request.
 */
public class DynamodbQueryKeyFinder {

    /**
     * Searches for the most selective key. Keys that are not used in an equality comparision are sorted out.
     * 
     * @param selection     selection to analyse
     * @param availableKeys list of possible DynamoDB keys or indexes
     * @return most selective key or null if non did fit
     */
    public DynamodbKey findMostSelectiveKey(final QueryPredicate<DynamodbNodeVisitor> selection,
            final List<DynamodbKey> availableKeys) {
        final List<DynamodbKey> suitableKeys = extractSuitableKeys(selection, availableKeys);
        return findMostSelectiveBySearchKey(selection, suitableKeys);
    }

    private List<DynamodbKey> extractSuitableKeys(final QueryPredicate<DynamodbNodeVisitor> selection,
            final List<DynamodbKey> availableKeys) {
        return availableKeys.stream().filter(key -> doesKeysPartitionKeyEqualitySelection(key, selection))
                .collect(Collectors.toList());
    }

    private boolean doesKeysPartitionKeyEqualitySelection(final DynamodbKey key,
            final QueryPredicate<DynamodbNodeVisitor> selection) {
        try {
            final QueryPredicate<DynamodbNodeVisitor> partitionKeySelection = new DynamodbQuerySelectionFilter()
                    .filter(selection, List.of(key.getPartitionKey()));
            final int partitionKeyRating = new DynamodbQuerySelectionRater().rate(partitionKeySelection);
            return partitionKeyRating == DynamodbQuerySelectionRater.RATING_EQUALITY;
        } catch (final DynamodbQuerySelectionFilterException exception) {
            return false;
        }
    }

    private DynamodbKey findMostSelectiveBySearchKey(final QueryPredicate<DynamodbNodeVisitor> selection,
            final List<DynamodbKey> suitableKeys) {
        int bestKeysRating = -1;
        DynamodbKey bestKey = null;
        for (final DynamodbKey key : suitableKeys) {
            try {
                final int keysRating = rateSortKeySelectivity(selection, key);
                if (keysRating > bestKeysRating) {
                    bestKeysRating = keysRating;
                    bestKey = key;
                }
            } catch (final DynamodbQuerySelectionFilterException exception) {
                // we just ignore this key if it does not fit.
            }
        }
        return bestKey;
    }

    private int rateSortKeySelectivity(final QueryPredicate<DynamodbNodeVisitor> selection, final DynamodbKey key) {
        if (!key.hasSortKey()) {
            return DynamodbQuerySelectionRater.RATING_NO_SELECTIVITY; // as this key has no search key it does not have
                                                                      // any selectivity
        }
        final QueryPredicate<DynamodbNodeVisitor> keySelection = new DynamodbQuerySelectionFilter().filter(selection,
                List.of(key.getSortKey()));
        return new DynamodbQuerySelectionRater().rate(keySelection);
    }
}
