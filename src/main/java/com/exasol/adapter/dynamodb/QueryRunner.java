package com.exasol.adapter.dynamodb;

import java.util.Map;
import java.util.stream.Stream;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.exasol.ExaConnectionInformation;
import com.exasol.adapter.dynamodb.queryresultschema.QueryResultTableSchema;
import com.exasol.dynamodb.DynamodbConnectionFactory;

public class QueryRunner {

    private final ExaConnectionInformation connectionSettings;

    public QueryRunner(final ExaConnectionInformation connectionSettings) {

        this.connectionSettings = connectionSettings;
    }

    private AmazonDynamoDB getConnection() {
        return new DynamodbConnectionFactory().getLowLevelConnection(this.connectionSettings.getAddress(),
                this.connectionSettings.getUser(), this.connectionSettings.getPassword());
    }

    public Stream<Map<String, AttributeValue>> runQuery(final QueryResultTableSchema query) {

        final AmazonDynamoDB client = getConnection();
        return client.scan(new ScanRequest(query.getFromTable().getRemoteName())).getItems().stream();

        // 2. select method (selection)
        // 3. add projection
        // 3. run
    }
}
