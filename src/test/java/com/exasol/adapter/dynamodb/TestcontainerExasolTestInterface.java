package com.exasol.adapter.dynamodb;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpRequest;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import com.exasol.LogProxy;
import com.exasol.bucketfs.Bucket;
import com.exasol.bucketfs.BucketAccessException;
import com.exasol.containers.ExasolContainer;
import com.exasol.jacoco.JacocoServer;
import com.github.dockerjava.api.model.ContainerNetwork;

/**
 * This class provides methods simplified methods for running typical commands on an Exasol test container.
 */
public class TestcontainerExasolTestInterface extends AbstractExasolTestInterface {
    private final ExasolContainer<? extends ExasolContainer<?>> container;

    /**
     * Create an instance of {@link TestcontainerExasolTestInterface}.
     * 
     * @param container exasol test container
     * @throws SQLException on SQL error
     */
    public TestcontainerExasolTestInterface(final ExasolContainer<? extends ExasolContainer<?>> container)
            throws SQLException, IOException {
        super(container.createConnectionForUser(container.getUsername(), container.getPassword()));
        this.container = container;
        JacocoServer.startIfNotRunning();
        LogProxy.startIfNotRunning();
    }

    @Override
    protected String getTestHostIpAddress() {
        final Map<String, ContainerNetwork> networks = this.container.getContainerInfo().getNetworkSettings()
                .getNetworks();
        if (networks.size() == 0) {
            return null;
        }
        return networks.values().iterator().next().getGateway();
    }

    @Override
    protected void uploadFileToBucketfs(final Path localPath, final String bucketPath)
            throws InterruptedException, BucketAccessException, TimeoutException {
        final Bucket bucket = this.container.getDefaultBucket();
        bucket.uploadFile(localPath, bucketPath);
    }
}
