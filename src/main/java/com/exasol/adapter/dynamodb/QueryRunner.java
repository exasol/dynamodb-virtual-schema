package com.exasol.adapter.dynamodb;

import java.util.Map;
import java.util.stream.Stream;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.exasol.ExaConnectionInformation;
import com.exasol.adapter.dynamodb.queryresultschema.QueryResultTableSchema;
import com.exasol.dynamodb.DynamodbConnectionFactory;

/**
 * This class runs a DynamoDB query to fetch the data requested in a {@link QueryResultTableSchema}.
 */
public class QueryRunner {
    private final ExaConnectionInformation connectionSettings;

    /**
     * Creates an instance of {@link QueryRunner}.
     *
     * @param connectionSettings connection information for the connection to DynamoDB
     */
    public QueryRunner(final ExaConnectionInformation connectionSettings) {

        this.connectionSettings = connectionSettings;
    }

    /**
     * Executes a query on DynamoDB.
     * 
     * @param query requested information
     * @return stream of results
     */
    public Stream<Map<String, AttributeValue>> runQuery(final QueryResultTableSchema query) {
        final AmazonDynamoDB client = getConnection();
        return client.scan(new ScanRequest(query.getFromTable().getRemoteName())).getItems().stream();
    }

    private AmazonDynamoDB getConnection() {
        return new DynamodbConnectionFactory().getLowLevelConnection(this.connectionSettings.getAddress(),
                this.connectionSettings.getUser(), this.connectionSettings.getPassword());
    }
}
