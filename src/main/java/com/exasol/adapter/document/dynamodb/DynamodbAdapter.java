package com.exasol.adapter.document.dynamodb;

import com.exasol.ExaConnectionInformation;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.capabilities.LiteralCapability;
import com.exasol.adapter.capabilities.MainCapability;
import com.exasol.adapter.capabilities.PredicateCapability;
import com.exasol.adapter.document.DocumentAdapter;
import com.exasol.adapter.document.QueryPlanner;
import com.exasol.adapter.document.dynamodbmetadata.BaseDynamodbTableMetadataFactory;
import com.exasol.adapter.document.mapping.TableKeyFetcher;
import com.exasol.adapter.document.mapping.dynamodb.DynamodbTableKeyFetcher;
import com.exasol.dynamodb.DynamodbConnectionFactory;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * DynamoDB Virtual Schema adapter.
 */
public class DynamodbAdapter extends DocumentAdapter {
    public static final String ADAPTER_NAME = "DYNAMO_DB";

    private static final Capabilities CAPABILITIES = Capabilities.builder()
            .addMain(MainCapability.FILTER_EXPRESSIONS, MainCapability.SELECTLIST_PROJECTION)
            .addPredicate(PredicateCapability.EQUAL, PredicateCapability.LESS, PredicateCapability.LESSEQUAL,
                    PredicateCapability.AND, PredicateCapability.OR, PredicateCapability.NOT)
            .addLiteral(LiteralCapability.STRING, LiteralCapability.NULL, LiteralCapability.BOOL,
                    LiteralCapability.DOUBLE, LiteralCapability.EXACTNUMERIC)
            .build();

    private DynamoDbClient getDynamoDBClient(final ExaConnectionInformation connectionInformation) {
        return new DynamodbConnectionFactory().getConnection(connectionInformation);
    }

    @Override
    protected TableKeyFetcher getTableKeyFetcher(final ExaConnectionInformation connectionInformation) {
        final BaseDynamodbTableMetadataFactory metadataFactory = new BaseDynamodbTableMetadataFactory(
                getDynamoDBClient(connectionInformation));
        return new DynamodbTableKeyFetcher(metadataFactory);
    }

    @Override
    protected QueryPlanner getQueryPlanner(final ExaConnectionInformation connectionInformation) {
        return new DynamodbQueryPlanner(getDynamoDBClient(connectionInformation));
    }

    @Override
    protected String getAdapterName() {
        return ADAPTER_NAME;
    }

    @Override
    protected Capabilities getCapabilities() {
        return CAPABILITIES;
    }
}
