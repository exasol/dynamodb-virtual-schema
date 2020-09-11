package com.exasol.adapter.document;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides test setups as configured in the system properties.
 */
public class IntegrationTestSetup {
    public static final String TUTORIAL = "You can specify a different test setup using -Dtests.testSetup=. "
            + "Possible values are testcontainers, local, and aws.";
    private static final Logger LOGGER = LoggerFactory.getLogger(IntegrationTestSetup.class);

    /**
     * Get a {@link ExasolTestInterface} according to the users configuration:
     *
     * Configure test backend using -Dtests.testSetup=
     *
     * Possible values are: testcontainers, docker, and aws
     * 
     * @return {@link ExasolTestInterface}
     */
    public ExasolTestInterface getExasolTestInterface()
            throws IOException, SQLException, NoSuchAlgorithmException, KeyManagementException {
        final String property = System.getProperty("tests.testSetup");
        if (property == null) {
            LOGGER.info("No tests setup was specified. Using defualt tescontainers test setup. " + TUTORIAL);
            return new TestcontainerExasolTestInterface();
        } else if (property.equalsIgnoreCase("testcontainers")) {
            LOGGER.info("Using testcontainers test setup. " + TUTORIAL);
            return new TestcontainerExasolTestInterface();
        } else if (property.equalsIgnoreCase("aws")) {
            LOGGER.info("Using AWS test setup. " + TUTORIAL);
            return new AwsExasolTestInterface();
        } else if (property.equalsIgnoreCase("local")) {
            LOGGER.info("Using local test setup. " + TUTORIAL);
            return new LocalExasolTestInterface();
        } else {
            throw new IllegalArgumentException("Unknown test setup \"" + property + "\". " + TUTORIAL);
        }
    }

    public DynamodbTestInterface getDynamodbTestInterface()
            throws DynamodbTestInterface.NoNetworkFoundException, IOException, URISyntaxException {
        final String property = System.getProperty("tests.testSetup");
        if (property == null) {
            LOGGER.info("No tests setup was specified. Using defualt tescontainers test setup. " + TUTORIAL);
            return new TestcontainerDynamodbTestInterface.Builder().build();
        } else if (property.equalsIgnoreCase("testcontainers")) {
            LOGGER.info("Using testcontainers test setup. " + TUTORIAL);
            return new TestcontainerDynamodbTestInterface.Builder().build();
        } else if (property.equalsIgnoreCase("aws")) {
            LOGGER.info("Using AWS test setup. " + TUTORIAL);
            return new AwsDynamodbTestInterface.Builder().build();
        } else if (property.equalsIgnoreCase("local")) {
            LOGGER.info("Using local test setup. " + TUTORIAL);
            return new LocalDynamodbTestInterface.Builder().build();
        } else {
            throw new IllegalArgumentException("Unknown test setup \"" + property + "\". " + TUTORIAL);
        }
    }
}
