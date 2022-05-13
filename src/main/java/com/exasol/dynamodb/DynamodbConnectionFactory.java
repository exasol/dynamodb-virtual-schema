package com.exasol.dynamodb;

import java.net.URI;
import java.net.URISyntaxException;

import com.exasol.adapter.document.dynamodb.connection.DynamodbConnectionProperties;
import com.exasol.errorreporting.ExaError;

import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

/**
 * This class represents a factory that creates connections to DynamoDB.
 */
public class DynamodbConnectionFactory {
    /**
     * Create an AmazonDynamoDB (low level api client) for a given uri, user and key.
     *
     * @param connectionProperties connection properties
     * @return {@link DynamoDbClient}
     */
    public DynamoDbClient getConnection(final DynamodbConnectionProperties connectionProperties) {
        final DynamoDbClientBuilder clientBuilder = DynamoDbClient.builder();
        clientBuilder.region(Region.of(connectionProperties.getAwsRegion()));
        if (connectionProperties.getAwsEndpointOverride() != null) {
            final String url = (connectionProperties.isUseSsl() ? "https://" : "http://")
                    + connectionProperties.getAwsEndpointOverride();
            clientBuilder.endpointOverride(parseDynamodbUri(url));
        }
        final AwsCredentials awsCredentials = getAwsCredentials(connectionProperties.getAwsAccessKeyId(),
                connectionProperties.getAwsSecretAccessKey(), connectionProperties.getAwsSessionToken());
        clientBuilder.credentialsProvider(StaticCredentialsProvider.create(awsCredentials));
        return clientBuilder.build();
    }

    private URI parseDynamodbUri(final String uri) {
        try {
            return new URI(uri);
        } catch (final URISyntaxException exception) {
            throw new IllegalArgumentException(ExaError.messageBuilder("E-VS-DY-1")
                    .message("Invalid DynamoDB URI {{URI}}.").parameter("URI", uri)
                    .mitigation(
                            "Please set a valid value for awsEndpointOverride in the format HOST:PORT. The value must not start with http:// or https://.")
                    .toString(), exception);
        }
    }

    private AwsCredentials getAwsCredentials(final String user, final String key, final String sessionToken) {
        if (sessionToken == null) {
            return AwsBasicCredentials.create(user, key);
        } else {
            return AwsSessionCredentials.create(user, key, sessionToken);
        }
    }
}
