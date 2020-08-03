package com.exasol.dynamodb;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;

import com.exasol.ExaConnectionInformation;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

/**
 * This class represents a factory that creates connections to DynamoDB.
 */
public class DynamodbConnectionFactory {
    private static final String AWS_PREFIX = "aws:";
    private static final String TOKEN_SEPARATOR = "##TOKEN##";

    public static String buildPassWithTokenSeparator(final String key, final Optional<String> token) {
        if (token.isEmpty()) {
            return key;
        } else {
            return key + TOKEN_SEPARATOR + token.get();
        }
    }

    /**
     * Create an AmazonDynamoDB (low level api client) for a given uri, user and key.
     *
     * @param uri  either aws:<REGION> or the address of a local DynamoDB server (e.g. http://localhost:8000)
     * @param user aws credential id
     * @param key  aws credential key
     * @return {@link DynamoDbClient}
     */
    public DynamoDbClient getConnection(final String uri, final String user, final String key,
            final Optional<String> sessionToken) throws URISyntaxException {

        final DynamoDbClientBuilder clientBuilder = DynamoDbClient.builder();
        if (uri.startsWith(AWS_PREFIX)) {
            clientBuilder.region(Region.of(uri.replace(AWS_PREFIX, "")));
        } else {
            clientBuilder.region(Region.EU_CENTRAL_1);// for a local DynamoDB region does not matter anyway
            clientBuilder.endpointOverride(new URI(uri));
        }
        final AwsCredentials awsCredentials = getAwsCredentials(user, key, sessionToken);
        clientBuilder.credentialsProvider(StaticCredentialsProvider.create(awsCredentials));
        return clientBuilder.build();
    }

    private AwsCredentials getAwsCredentials(final String user, final String key, final Optional<String> sessionToken) {
        if (sessionToken.isEmpty()) {
            if (key.contains(TOKEN_SEPARATOR)) {
                final String[] parts = key.split(TOKEN_SEPARATOR);
                if (parts.length != 2) {
                    throw new IllegalArgumentException(
                            TOKEN_SEPARATOR + " must be used like: <PASS>" + TOKEN_SEPARATOR + "<TOKEN>");
                }
                return getAwsCredentials(user, parts[0], Optional.of(parts[1]));
            }
            return AwsBasicCredentials.create(user, key);
        } else {
            return AwsSessionCredentials.create(user, key, sessionToken.get());
        }
    }

    /**
     * Create an AmazonDynamoDB (low level api client) with connection settings from an Exasol
     * {@link ExaConnectionInformation} object.
     *
     * @param exaConnectionInformation connection settings
     * @return {@link DynamoDbClient} (low level api client)
     */
    public DynamoDbClient getConnection(final ExaConnectionInformation exaConnectionInformation)
            throws URISyntaxException {
        return getConnection(exaConnectionInformation.getAddress(), exaConnectionInformation.getUser(),
                exaConnectionInformation.getPassword(), Optional.empty());
    }
}
