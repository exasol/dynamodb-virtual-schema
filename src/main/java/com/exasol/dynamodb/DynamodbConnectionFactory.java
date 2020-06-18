package com.exasol.dynamodb;

import java.util.Optional;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.exasol.ExaConnectionInformation;

/**
 * This class represents a factory that creates connections to DynamoDB.
 */
public class DynamodbConnectionFactory {
    private static final String AWS_PREFIX = "aws:";
    private static final String AWS_LOCAL_REGION = "eu-central-1";
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
     * @return {@link AmazonDynamoDB} (low level api client)
     */
    public AmazonDynamoDB getLowLevelConnection(final String uri, final String user, final String key,
            final Optional<String> sessionToken) {
        final AWSCredentials awsCredentials = getAwsCredentials(user, key, sessionToken);
        final AmazonDynamoDBClientBuilder clientBuilder = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials));
        if (uri.startsWith(AWS_PREFIX)) {
            clientBuilder.withRegion(uri.replace(AWS_PREFIX, ""));
        } else {
            clientBuilder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(uri, AWS_LOCAL_REGION));
        }
        return clientBuilder.build();
    }

    private AWSCredentials getAwsCredentials(final String user, final String key, final Optional<String> sessionToken) {
        if (sessionToken.isEmpty()) {
            if (key.contains(TOKEN_SEPARATOR)) {
                final String[] parts = key.split(TOKEN_SEPARATOR);
                if (parts.length != 2) {
                    throw new IllegalArgumentException(
                            TOKEN_SEPARATOR + " must be used like: <PASS>" + TOKEN_SEPARATOR + "<TOKEN>");
                }
                return getAwsCredentials(user, parts[0], Optional.of(parts[1]));
            }
            return new BasicAWSCredentials(user, key);
        } else {
            return new BasicSessionCredentials(user, key, sessionToken.get());
        }
    }

    /**
     * Create an AmazonDynamoDB (low level api client) with connection settings from an Exasol
     * {@link ExaConnectionInformation} object.
     *
     * @param exaConnectionInformation connection settings
     * @return {@link AmazonDynamoDB} (low level api client)
     */
    public AmazonDynamoDB getLowLevelConnection(final ExaConnectionInformation exaConnectionInformation) {
        return getLowLevelConnection(exaConnectionInformation.getAddress(), exaConnectionInformation.getUser(),
                exaConnectionInformation.getPassword(), Optional.empty());
    }

    /**
     * Create a DynamoDB (document api client) for a given uri, user and key. for details see
     * {@link #getLowLevelConnection(String, String, String, Optional)}.
     *
     * @return DynamoDB (document api client)
     */
    public DynamoDB getDocumentConnection(final String uri, final String user, final String key,
            final Optional<String> sessionToken) {
        return new DynamoDB(getLowLevelConnection(uri, user, key, sessionToken));
    }
}
