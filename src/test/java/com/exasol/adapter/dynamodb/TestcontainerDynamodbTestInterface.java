package com.exasol.adapter.dynamodb;

import java.net.URISyntaxException;
import java.util.Map;
import java.util.Optional;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import com.github.dockerjava.api.model.ContainerNetwork;

/**
 * DynamoDB test interface for a local DynamoDB that is automatically started via testcontainers.
 */
public class TestcontainerDynamodbTestInterface extends DynamodbTestInterface {
    private static final String LOCAL_DYNAMO_USER = "fakeMyKeyId";
    private static final String LOCAL_DYNAMO_PASS = "fakeSecretAccessKey";
    private static final String LOCAL_DYNAMO_PORT = "8000";

    private final GenericContainer LOCAL_DYNAMO;

    /**
     * Constructor using default login credentials for the local dynamodb docker instance.
     */
    private TestcontainerDynamodbTestInterface(final GenericContainer dynamodbContainer, final Network network)
            throws NoNetworkFoundException, URISyntaxException {
        super(getDockerNetworkUrlForLocalDynamodb(dynamodbContainer, network), LOCAL_DYNAMO_USER, LOCAL_DYNAMO_PASS,
                Optional.empty());
        this.LOCAL_DYNAMO = dynamodbContainer;
    }

    private static String getDockerNetworkUrlForLocalDynamodb(final GenericContainer localDynamo,
            final Network thisNetwork) throws NoNetworkFoundException {
        final Map<String, ContainerNetwork> networks = localDynamo.getContainerInfo().getNetworkSettings()
                .getNetworks();
        for (final ContainerNetwork network : networks.values()) {
            if (thisNetwork.getId().equals(network.getNetworkID())) {
                return "http://" + network.getIpAddress() + ":" + LOCAL_DYNAMO_PORT;
            }
        }
        throw new NoNetworkFoundException();
    }

    @Override
    public void teardown() {
        this.LOCAL_DYNAMO.stop();
    }

    public static class Builder {
        public TestcontainerDynamodbTestInterface build() throws NoNetworkFoundException, URISyntaxException {
            final Network network = TestcontainerNetworkProvider.getNetwork();
            final GenericContainer container = new GenericContainer<>("amazon/dynamodb-local").withExposedPorts(8000)
                    .withNetwork(TestcontainerNetworkProvider.getNetwork()).withNetworkAliases("dynamo")
                    .withCommand("-jar DynamoDBLocal.jar -sharedDb -dbPath .");
            container.start();
            return new TestcontainerDynamodbTestInterface(container, network);
        }
    }
}
