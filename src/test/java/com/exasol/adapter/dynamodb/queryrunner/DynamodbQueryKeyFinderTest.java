package com.exasol.adapter.dynamodb.queryrunner;

import java.util.List;
import java.util.Optional;

import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbKey;
import com.exasol.adapter.dynamodb.remotetablequery.LogicalOperator;
import com.exasol.adapter.dynamodb.remotetablequery.ColumnLiteralComparisonPredicate;
import com.exasol.adapter.dynamodb.remotetablequery.NoPredicate;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class DynamodbQueryKeyFinderTest {
    private static final String PARTITION_KEY = "partitionKey";
    private static final String SORT_KEY_NAME = "sortKey";
    private static final ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> PARTITION_KEY_COMPARISON = TestSetup
            .getCompareForColumn(PARTITION_KEY);
    private static final ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> SORT_KEY_COMPARISON = TestSetup
            .getCompareForColumn(SORT_KEY_NAME);

    private static final DynamodbKey KEY_WITH_NO_SORT_KEY = new DynamodbKey(PARTITION_KEY, Optional.empty());
    private static final DynamodbKey KEY_WITH_SORT_KEY = new DynamodbKey(PARTITION_KEY, Optional.of(SORT_KEY_NAME));
    final List<DynamodbKey> KEYS = List.of(KEY_WITH_NO_SORT_KEY, KEY_WITH_SORT_KEY);

    @Test
    void testFindMoreSelective() {
        final LogicalOperator<DynamodbNodeVisitor> selection = new LogicalOperator<>(
                List.of(PARTITION_KEY_COMPARISON, SORT_KEY_COMPARISON), LogicalOperator.Operator.AND);
        final DynamodbKey result = new DynamodbQueryKeyFinder().findMostSelectiveKey(selection, this.KEYS);
        assertThat(result, equalTo(KEY_WITH_SORT_KEY));
    }

    @Test
    void testKeysWithNoPrimarySelectionAreFiltered(){
        final DynamodbKey result = new DynamodbQueryKeyFinder().findMostSelectiveKey(new NoPredicate<>(), this.KEYS);
        assertThat(result, equalTo(null));
    }
}