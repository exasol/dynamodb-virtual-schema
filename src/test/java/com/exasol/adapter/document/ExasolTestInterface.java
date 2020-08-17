package com.exasol.adapter.document;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;

import com.exasol.bucketfs.BucketAccessException;

/**
 * Unified interface for Exasol for different test platforms.
 */
public interface ExasolTestInterface {

    public void teardown();

    /**
     * Hacky method for retrieving the host address for access from inside the docker container.
     */
    public String getTestHostIpAddress();

    public void uploadFileToBucketfs(Path localPath, String bucketPath)
            throws InterruptedException, BucketAccessException, TimeoutException;

    public Connection getConnection() throws SQLException, IOException;
}
