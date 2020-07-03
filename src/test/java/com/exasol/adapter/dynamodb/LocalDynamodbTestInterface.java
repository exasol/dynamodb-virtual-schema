package com.exasol.adapter.dynamodb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

/**
 * DynamoDB test interface for a local DynamoDB created with the docker-compose file from test/java/resources.
 */
public class LocalDynamodbTestInterface extends DynamodbTestInterface {

    private LocalDynamodbTestInterface(final String containerIp) {
        super("http://" + containerIp + ":8000", "fake", "fake", Optional.empty());
    }

    @Override
    public void teardown() {

    }

    public static class Builder {
        LocalDynamodbTestInterface build() throws IOException {
            return new LocalDynamodbTestInterface(getContainersIp());
        }

        @Test
        private String getContainersIp() throws IOException {
            final Runtime runtime = Runtime.getRuntime();
            final String[] command = { "docker", "inspect", "-f",
                    "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", "resources_dynamodb_1" };
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
        }
    }
}
