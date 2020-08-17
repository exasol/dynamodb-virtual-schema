package com.exasol.adapter.document;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Base64;
import java.util.stream.Collectors;

import com.exasol.LogProxy;
import com.exasol.jacoco.JacocoServer;

/**
 * Exasol test interface for a local Exasol database created with the docker-compose file from test/java/resources.
 */
public class LocalExasolTestInterface extends AbstractExasolTestInterface {
    private final String bucketfsWritePassword;
    private final String testHostIpAddress;

    public LocalExasolTestInterface() throws IOException, SQLException {
        this.bucketfsWritePassword = extractWritePasswordFromExaconf();
        this.testHostIpAddress = lookupTestHostIpAddress();
        JacocoServer.startIfNotRunning();
        LogProxy.startIfNotRunning();
        new ExasolTestDatabaseBuilder(this).cleanup();
    }

    @Override
    public void teardown() {

    }

    @Override
    public String getTestHostIpAddress() {
        return this.testHostIpAddress;
    }

    private String lookupTestHostIpAddress() {
        try {
            final Runtime runtime = Runtime.getRuntime();
            final String[] command = { "docker", "inspect", "-f",
                    "{{range .NetworkSettings.Networks}}{{.Gateway}}{{end}}", "resources_dynamodb_1" };
            final Process dockerComposeProcess = runtime.exec(command);
            final BufferedReader stdInput = new BufferedReader(
                    new InputStreamReader(dockerComposeProcess.getInputStream()));
            final String output = stdInput.readLine();
            if (output == null) {
                final StringBuilder errorMessage = new StringBuilder("Could not read docker-compose output:\n");
                final BufferedReader stdError = new BufferedReader(
                        new InputStreamReader(dockerComposeProcess.getErrorStream()));
                errorMessage.append(stdError.lines().collect(Collectors.joining("\n")));
                throw new IllegalStateException(errorMessage.toString());
            }
            return output;
        } catch (final IOException exception) {
            return null;
        }
    }

    @Override
    protected String getContainerUrl() {
        return "localhost";
    }

    @Override
    protected int getBucketFsPort() {
        return 6583;
    }

    @Override
    protected String getBucketFsReadPassword() {
        return null;
    }

    @Override
    protected String getBucketFsWritePassword() {
        return this.bucketfsWritePassword;
    }

    @Override
    public Connection getConnection() throws SQLException, IOException {
        return DriverManager.getConnection("jdbc:exa:localhost:8888;schema=SYS", "sys", "exasol");
    }

    private String extractWritePasswordFromExaconf() throws IOException {
        final Runtime runtime = Runtime.getRuntime();
        final String[] command = { "docker-compose", "-f", "./src/test/resources/docker-compose.yml", "exec", "-T",
                "exasol", "awk", "/WritePasswd/{ print $3; }", "/exa/etc/EXAConf" };
        final Process dockerComposeProcess = runtime.exec(command);
        final BufferedReader stdInput = new BufferedReader(
                new InputStreamReader(dockerComposeProcess.getInputStream()));
        final String output = stdInput.readLine();
        if (output == null) {
            final StringBuilder errorMessage = new StringBuilder("Could not read docker-compose output:\n");
            final BufferedReader stdError = new BufferedReader(
                    new InputStreamReader(dockerComposeProcess.getErrorStream()));
            errorMessage.append(stdError.lines().collect(Collectors.joining("\n")));
            throw new IllegalStateException(errorMessage.toString());
        }
        return new String(Base64.getDecoder().decode(output), StandardCharsets.UTF_8);
    }
}
