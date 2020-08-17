package com.exasol.adapter.document;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import com.exasol.LogProxy;
import com.exasol.bucketfs.Bucket;
import com.exasol.bucketfs.BucketAccessException;
import com.exasol.containers.ExasolContainer;
import com.exasol.jacoco.JacocoServer;
import com.github.dockerjava.api.model.ContainerNetwork;

/**
 * Exasol test interface for a local Exasol database that is automatically started via testcontainers.
 */
public class TestcontainerExasolTestInterface implements ExasolTestInterface {
    private final ExasolContainer<? extends ExasolContainer<?>> container;
    private static final Logger LOGGER = LoggerFactory.getLogger(TestcontainerExasolTestInterface.class);

    /**
     * Create an instance of {@link TestcontainerExasolTestInterface}.
     */
    public TestcontainerExasolTestInterface() throws IOException {

        this.container = new ExasolContainer<>().withNetwork(TestcontainerNetworkProvider.getNetwork())
                .withExposedPorts(8888).withLogConsumer(new Slf4jLogConsumer(LOGGER));
        JacocoServer.startIfNotRunning();
        LogProxy.startIfNotRunning();
        this.container.start();
    }

    @Override
    public void teardown() {
        this.container.getDockerClient().stopContainerCmd(this.container.getContainerId()).withTimeout(10).exec();
        this.container.stop();
    }

    @Override
    public String getTestHostIpAddress() {
        final Map<String, ContainerNetwork> networks = this.container.getContainerInfo().getNetworkSettings()
                .getNetworks();
        if (networks.size() == 0) {
            return null;
        }
        return networks.values().iterator().next().getGateway();
    }

    @Override
    public void uploadFileToBucketfs(final Path localPath, final String bucketPath)
            throws InterruptedException, BucketAccessException, TimeoutException {
        final Bucket bucket = this.container.getDefaultBucket();
        bucket.uploadFile(localPath, bucketPath);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.container.createConnectionForUser(this.container.getUsername(), this.container.getPassword());
    }
}
