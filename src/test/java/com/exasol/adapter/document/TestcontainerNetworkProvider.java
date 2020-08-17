package com.exasol.adapter.document;

import org.testcontainers.containers.Network;

/**
 * This class provides a single Testcontainer Network.
 */
public class TestcontainerNetworkProvider {
    private static final Network NETWORK = Network.newNetwork();

    /**
     * Get the Testcontainer network.
     * 
     * @return Testcontainer network
     */
    public static Network getNetwork() {
        return NETWORK;
    }
}
