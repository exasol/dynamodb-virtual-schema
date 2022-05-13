package com.exasol.adapter.document.dynamodb;

import static com.exasol.adapter.document.dynamodb.Constants.USER_GUIDE_URL;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.*;
import com.exasol.adapter.document.DocumentAdapterDialect;
import com.exasol.adapter.document.QueryPlanner;
import com.exasol.adapter.document.connection.ConnectionPropertiesReader;
import com.exasol.adapter.document.dynamodb.connection.DynamodbConnectionProperties;
import com.exasol.adapter.document.dynamodb.connection.DynamodbConnectionPropertiesReader;
import com.exasol.adapter.document.dynamodbmetadata.BaseDynamodbTableMetadataFactory;
import com.exasol.adapter.document.mapping.TableKeyFetcher;
import com.exasol.adapter.document.mapping.dynamodb.DynamodbTableKeyFetcher;
import com.exasol.dynamodb.DynamodbConnectionFactory;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

class DynamodbDocumentAdapterDialect implements DocumentAdapterDialect {
    /** Name of the dialect. */
    public static final String ADAPTER_NAME = "DYNAMO_DB";

    private static final Capabilities CAPABILITIES = Capabilities.builder()
            .addMain(MainCapability.FILTER_EXPRESSIONS, MainCapability.SELECTLIST_PROJECTION)
            .addPredicate(PredicateCapability.EQUAL, PredicateCapability.LESS, PredicateCapability.LESSEQUAL,
                    PredicateCapability.AND, PredicateCapability.OR, PredicateCapability.NOT)
            .addLiteral(LiteralCapability.STRING, LiteralCapability.NULL, LiteralCapability.BOOL,
                    LiteralCapability.DOUBLE, LiteralCapability.EXACTNUMERIC)
            .build();

    @Override
    public TableKeyFetcher getTableKeyFetcher(final ConnectionPropertiesReader connectionInformation) {
        final BaseDynamodbTableMetadataFactory metadataFactory = new BaseDynamodbTableMetadataFactory(
                getDynamoDBClient(connectionInformation));
        return new DynamodbTableKeyFetcher(metadataFactory);
    }

    @Override
    public QueryPlanner getQueryPlanner(final ConnectionPropertiesReader connectionInformation,
            final AdapterProperties adapterProperties) {
        return new DynamodbQueryPlanner(getDynamoDBClient(connectionInformation));
    }

    @Override
    public String getUserGuideUrl() {
        return USER_GUIDE_URL;
    }

    private DynamoDbClient getDynamoDBClient(final ConnectionPropertiesReader connectionInformation) {
        final DynamodbConnectionProperties connectionProperties = new DynamodbConnectionPropertiesReader()
                .read(connectionInformation);
        return new DynamodbConnectionFactory().getConnection(connectionProperties);
    }

    @Override
    public String getAdapterName() {
        return ADAPTER_NAME;
    }

    @Override
    public Capabilities getCapabilities() {
        return CAPABILITIES;
    }
}
