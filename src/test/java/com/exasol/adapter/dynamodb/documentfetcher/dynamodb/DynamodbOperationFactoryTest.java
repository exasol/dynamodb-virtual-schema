package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dynamodb.DynamodbTestInterface;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbPrimaryIndex;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbSecondaryIndex;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbTableMetadata;
import com.exasol.adapter.dynamodb.remotetablequery.RemoteTableQuery;

class DynamodbOperationFactoryTest {
    private static final DynamodbTableMetadata tableMetadata = new DynamodbTableMetadata(
            new DynamodbPrimaryIndex(BasicMappingSetup.PRIMARY_KEY_NAME, Optional.empty()), List.of(),
            List.of(new DynamodbSecondaryIndex(BasicMappingSetup.INDEX_PARTITION_KEY,
                    Optional.of(BasicMappingSetup.INDEX_SORT_KEY), BasicMappingSetup.INDEX_NAME)));
    private static BasicMappingSetup basicMappingSetup;

    @BeforeAll
    static void beforeAll() throws DynamodbTestInterface.NoNetworkFoundException, IOException, AdapterException {
        basicMappingSetup = new BasicMappingSetup();
    }

    @Test
    void testSelectAll() {
        final RemoteTableQuery<DynamodbNodeVisitor> documentQuery = basicMappingSetup.getSelectAllQuery();
        final DynamodbScanOperationPlan scanPlan = (DynamodbScanOperationPlan) new DynamodbOperationFactory()
                .planQuery(documentQuery, tableMetadata);
        assertThat(scanPlan.getScanRequest().getTableName(), equalTo(basicMappingSetup.tableMapping.getRemoteName()));
    }

    @Test
    void testSecondaryIndexQuery() {
        final String publisher = "jb books";
        final RemoteTableQuery<DynamodbNodeVisitor> documentQuery = basicMappingSetup.getQueryForPublisher(publisher);
        final DynamodbQueryOperationPlan dynamodbQueryPlan = (DynamodbQueryOperationPlan) new DynamodbOperationFactory()
                .planQuery(documentQuery, tableMetadata);
        assertAll(//
                () -> assertThat(dynamodbQueryPlan.getQueryRequest().getTableName(),
                        equalTo(basicMappingSetup.tableMapping.getRemoteName())),
                () -> assertThat(dynamodbQueryPlan.getQueryRequest().getIndexName(), equalTo("publisherIndex")),
                () -> assertThat(dynamodbQueryPlan.getQueryRequest().getKeyConditionExpression(),
                        equalTo("publisher = :0")),
                () -> assertThat(dynamodbQueryPlan.getQueryRequest().getExpressionAttributeValues().get(":0").getS(),
                        equalTo(publisher))//
        );
    }

    @Test
    void testRangeQueryWithPrimaryKey() {
        final String publisher = "jb books";
        final double price = 10.1;
        final DynamodbTableMetadata tableMetadata = new DynamodbTableMetadata(
                new DynamodbPrimaryIndex("publisher", Optional.of("price")), List.of(),
                List.of(new DynamodbSecondaryIndex(BasicMappingSetup.INDEX_PARTITION_KEY,
                        Optional.of(BasicMappingSetup.INDEX_SORT_KEY), BasicMappingSetup.INDEX_NAME)));

        final RemoteTableQuery<DynamodbNodeVisitor> documentQuery = basicMappingSetup
                .getQueryForMinPriceAndPublisher(price, publisher);
        final DynamodbQueryOperationPlan dynamodbQueryPlan = (DynamodbQueryOperationPlan) new DynamodbOperationFactory()
                .planQuery(documentQuery, tableMetadata);
        assertAll(//
                () -> assertThat(dynamodbQueryPlan.getQueryRequest().getTableName(),
                        equalTo(basicMappingSetup.tableMapping.getRemoteName())),
                () -> assertThat(dynamodbQueryPlan.getQueryRequest().getKeyConditionExpression(),
                        equalTo("price > :0 and publisher = :1")),
                () -> assertThat(dynamodbQueryPlan.getQueryRequest().getExpressionAttributeValues().get(":0").getN(),
                        equalTo(String.valueOf(price))),
                () -> assertThat(dynamodbQueryPlan.getQueryRequest().getExpressionAttributeValues().get(":1").getS(),
                        equalTo(publisher))//
        );
    }
}