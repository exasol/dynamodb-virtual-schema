package com.exasol.adapter.document;

import java.net.URISyntaxException;
import java.util.Optional;

import com.exasol.dynamodb.DynamodbContainer;

/**
 * DynamoDB test interface for a local DynamoDB that is automatically started via testcontainers.
 */
public class TestcontainerDynamodbTestDbBuilder extends DynamodbTestDbBuilder {
    private static final String LOCAL_DYNAMO_USER = "fakeMyKeyId";
    private static final String LOCAL_DYNAMO_PASS = "fakeSecretAccessKey";

    /**
     * Constructor using default login credentials for the local dynamodb docker instance.
     */
    public TestcontainerDynamodbTestDbBuilder(final DynamodbContainer dynamodbContainer) throws URISyntaxException {
        super("http://" + dynamodbContainer.getHost() + ":" + dynamodbContainer.getPort(), LOCAL_DYNAMO_USER,
                LOCAL_DYNAMO_PASS, Optional.empty());
    }
}
