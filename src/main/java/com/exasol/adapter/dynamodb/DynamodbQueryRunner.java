package com.exasol.adapter.dynamodb;

import java.util.stream.Stream;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.exasol.ExaConnectionInformation;
import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbMap;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.queryresultschema.QueryResultTableSchema;
import com.exasol.dynamodb.DynamodbConnectionFactory;

/**
 * This class runs a DynamoDB query to fetch the data requested in a {@link QueryResultTableSchema}.
 */
public class DynamodbQueryRunner {
    private final ExaConnectionInformation connectionSettings;

    /**
     * Creates an instance of {@link DynamodbQueryRunner}.
     *
     * @param connectionSettings connection information for the connection to DynamoDB
     */
    public DynamodbQueryRunner(final ExaConnectionInformation connectionSettings) {
        this.connectionSettings = connectionSettings;
    }

    /**
     * Executes a query on DynamoDB.
     * 
     * @param query requested information
     * @return stream of results
     */
    public Stream<DocumentNode<DynamodbNodeVisitor>> runQuery(final QueryResultTableSchema query) {
        final AmazonDynamoDB client = getConnection();
        final String tableName = query.getFromTable().getRemoteName();
        final ScanRequest scanRequest = new ScanRequest(tableName);
        return client.scan(scanRequest).getItems().stream().map(DynamodbMap::new);
    }

    private AmazonDynamoDB getConnection() {
        return new DynamodbConnectionFactory().getLowLevelConnection(this.connectionSettings.getAddress(),
                this.connectionSettings.getUser(), this.connectionSettings.getPassword());
    }
}
