package com.exasol.adapter.dynamodb.queryrunner;

import java.util.stream.Stream;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.exasol.ExaConnectionInformation;
import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbMap;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.documentquery.DocumentQuery;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbTableMetadata;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbTableMetadataFactory;
import com.exasol.adapter.sql.SqlStatement;
import com.exasol.dynamodb.DynamodbConnectionFactory;

/**
 * This class runs a DynamoDB query to fetch the data requested in a {@link DocumentQuery}.
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
    public Stream<DocumentNode<DynamodbNodeVisitor>> runQuery(final DocumentQuery schemaMappingPlan,
            final SqlStatement query) {
        final AmazonDynamoDB client = getConnection();
        final DynamodbTableMetadata tableMetadata = new DynamodbTableMetadataFactory().buildMetadataForTable(client,
                schemaMappingPlan.getFromTable().getRemoteName());
        final DynamodbQueryPlan queryPlan = new DynamodbQueryPlanner().planQuery(schemaMappingPlan, query,
                tableMetadata);
        return queryPlan.run(client).map(DynamodbMap::new);
    }

    private AmazonDynamoDB getConnection() {
        return new DynamodbConnectionFactory().getLowLevelConnection(this.connectionSettings.getAddress(),
                this.connectionSettings.getUser(), this.connectionSettings.getPassword());
    }
}
