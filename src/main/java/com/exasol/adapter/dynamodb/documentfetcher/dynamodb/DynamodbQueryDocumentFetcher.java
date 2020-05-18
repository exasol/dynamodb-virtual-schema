package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbIndex;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbSecondaryIndex;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbTableMetadata;
import com.exasol.adapter.dynamodb.remotetablequery.QueryPredicate;
import com.exasol.adapter.dynamodb.remotetablequery.RemoteTableQuery;

/**
 * This class represents a DynamoDB {@code QUERY} operation.
 */
public class DynamodbQueryDocumentFetcher extends AbstractDynamodbDocumentFetcher {
    private static final long serialVersionUID = -2972732083876517763L;
    private final QueryRequest queryRequest;

    /**
     * Creates an a {@link DynamodbQueryDocumentFetcher} if possible for the given query.
     *
     * @param remoteTableQuery query to build the plan for
     * @param tableMetadata    DynamoDB table metadata used for checking the primary key
     * @throws PlanDoesNotFitException if a DynamoDB {@code QUERY} operation can't fetch the data required by the query
     */
    public DynamodbQueryDocumentFetcher(final RemoteTableQuery<DynamodbNodeVisitor> remoteTableQuery,
            final DynamodbTableMetadata tableMetadata) {
        final DynamodbIndex mostRestrictedIndex = new DynamodbQueryIndexSelector()
                .findMostRestrictedIndex(remoteTableQuery.getSelection(), tableMetadata.getAllIndexes());
        abortIfNoFittingIndexWasFound(mostRestrictedIndex);
        this.queryRequest = new QueryRequest(remoteTableQuery.getFromTable().getRemoteName());
        setIndexToQuery(mostRestrictedIndex);
        addKeyConditionAndFilterExpression(remoteTableQuery, mostRestrictedIndex);
    }

    private void abortIfNoFittingIndexWasFound(final DynamodbIndex mostRestrictedIndex) {
        if (mostRestrictedIndex == null) {
            throw new PlanDoesNotFitException("Could not find a suitable key for a DynamoDB Query operation. "
                    + "Non of the keys did a equality selection with the partition key. "
                    + "Your can either add such a selection to your query or use a SCAN request.");
        }
    }

    private void setIndexToQuery(final DynamodbIndex mostRestrictedIndex) {
        if (mostRestrictedIndex instanceof DynamodbSecondaryIndex) {
            final DynamodbSecondaryIndex secondaryIndex = (DynamodbSecondaryIndex) mostRestrictedIndex;
            this.queryRequest.setIndexName(secondaryIndex.getIndexName());
        }
    }

    private void addKeyConditionAndFilterExpression(final RemoteTableQuery<DynamodbNodeVisitor> documentQuery,
            final DynamodbIndex mostRestrictedIndex) {
        final QueryPredicate<DynamodbNodeVisitor> selectionOnIndex = new DynamodbQuerySelectionFilter()
                .filter(documentQuery.getSelection(), getIndexPropertyNameWhitelist(mostRestrictedIndex));
        final DynamodbAttributeNamePlaceholderMapBuilder namePlaceholderBuilder = new DynamodbAttributeNamePlaceholderMapBuilder();
        final DynamodbAttributeValuePlaceholderMapBuilder valuePlaceholderBuilder = new DynamodbAttributeValuePlaceholderMapBuilder();
        final DynamodbFilterExpressionFactory filterExpressionFactory = new DynamodbFilterExpressionFactory();
        final String keyConditionExpression = filterExpressionFactory.buildFilterExpression(selectionOnIndex,
                namePlaceholderBuilder, valuePlaceholderBuilder);
        this.queryRequest.setKeyConditionExpression(keyConditionExpression);
        this.queryRequest.setExpressionAttributeNames(namePlaceholderBuilder.getValueMap());
        this.queryRequest.setExpressionAttributeValues(valuePlaceholderBuilder.getValueMap());
    }

    private List<String> getIndexPropertyNameWhitelist(final DynamodbIndex index) {
        if (index.hasSortKey()) {
            return List.of(index.getPartitionKey(), index.getSortKey());
        } else {
            return List.of(index.getPartitionKey());
        }
    }

    QueryRequest getQueryRequest() {
        return this.queryRequest;
    }

    @Override
    public Stream<Map<String, AttributeValue>> run(final AmazonDynamoDB client) {
        return client.query(this.queryRequest).getItems().stream();
    }
}
