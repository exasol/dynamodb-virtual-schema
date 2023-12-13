package com.exasol.dynamodb;

import org.testcontainers.containers.GenericContainer;

public class DynamodbContainer extends GenericContainer<DynamodbContainer> {
    private static final String DYNAMODB_IMAGE = "amazon/dynamodb-local:2.1.0";
    private static final int DYNAMODB_PORT = 8000;

    public DynamodbContainer() {
        super(DYNAMODB_IMAGE);
        this.withExposedPorts(DYNAMODB_PORT).withCommand("-jar DynamoDBLocal.jar -sharedDb -dbPath .");
    }

    /**
     * Get the port under which DynamoDB is reachable at the host.
     * 
     * @return port DynamoDB is mapped to
     */
    public int getPort() {
        return getMappedPort(DYNAMODB_PORT);
    }
}
