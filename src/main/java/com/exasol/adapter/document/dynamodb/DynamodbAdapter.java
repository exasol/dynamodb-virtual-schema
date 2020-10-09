package com.exasol.adapter.document.dynamodb;

import com.exasol.ExaConnectionInformation;
import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.capabilities.LiteralCapability;
import com.exasol.adapter.capabilities.MainCapability;
import com.exasol.adapter.capabilities.PredicateCapability;
import com.exasol.adapter.document.DocumentAdapter;
import com.exasol.adapter.document.QueryPlanner;
import com.exasol.adapter.document.dynamodbmetadata.BaseDynamodbTableMetadataFactory;
import com.exasol.adapter.document.mapping.TableKeyFetcher;
import com.exasol.adapter.document.mapping.dynamodb.DynamodbTableKeyFetcher;
import com.exasol.adapter.request.GetCapabilitiesRequest;
import com.exasol.adapter.response.GetCapabilitiesResponse;
import com.exasol.dynamodb.DynamodbConnectionFactory;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * DynamoDB Virtual Schema adapter.
 */
public class DynamodbAdapter extends DocumentAdapter {
    public static final String ADAPTER_NAME = "DYNAMO_DB";

    private static final Capabilities CAPABILITIES = Capabilities.builder()
            .addMain(MainCapability.FILTER_EXPRESSIONS, MainCapability.SELECTLIST_PROJECTION)
            .addPredicate(PredicateCapability.EQUAL, PredicateCapability.LESS, PredicateCapability.LESSEQUAL)
            .addLiteral(LiteralCapability.STRING, LiteralCapability.NULL, LiteralCapability.BOOL,
                    LiteralCapability.DOUBLE, LiteralCapability.EXACTNUMERIC)
            .build();

    private DynamoDbClient getDynamoDBClient(final ExaConnectionInformation connectionInformation)
            throws AdapterException {
        try {
            return new DynamodbConnectionFactory().getConnection(connectionInformation);
        } catch (final Exception exception) {
            throw new AdapterException("Failed to connect DynamoDB. Cause: " + exception.getMessage(), exception);
        }
    }

    @Override
    public GetCapabilitiesResponse getCapabilities(final ExaMetadata exaMetadata,
            final GetCapabilitiesRequest getCapabilitiesRequest) {
        return GetCapabilitiesResponse //
                .builder()//
                .capabilities(CAPABILITIES)//
                .build();

    }

    @Override
    protected TableKeyFetcher getTableKeyFetcher(final ExaConnectionInformation connectionInformation)
            throws AdapterException {
        final BaseDynamodbTableMetadataFactory metadataFactory = new BaseDynamodbTableMetadataFactory(
                getDynamoDBClient(connectionInformation));
        return new DynamodbTableKeyFetcher(metadataFactory);
    }

    @Override
    protected QueryPlanner getQueryPlanner(final ExaConnectionInformation connectionInformation)
            throws AdapterException {
        return new DynamodbQueryPlanner(getDynamoDBClient(connectionInformation));
    }

    @Override
    protected String getAdapterName() {
        return ADAPTER_NAME;
    }
}
