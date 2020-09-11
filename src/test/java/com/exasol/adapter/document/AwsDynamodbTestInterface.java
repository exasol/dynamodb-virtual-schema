package com.exasol.adapter.document;

import java.net.URISyntaxException;
import java.util.Optional;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

/**
 * DynamoDB test interface using a DynamoDB running on AWS.
 */
public class AwsDynamodbTestInterface extends DynamodbTestInterface {

    private AwsDynamodbTestInterface(final String dynamoUrl, final String user, final String pass,
            final Optional<String> sessionToken) throws URISyntaxException {
        super(dynamoUrl, user, pass, sessionToken);
    }

    @Override
    public void teardown() {

    }

    public static class Builder {
        private static final String DEFAULT_REGION = "aws:eu-central-1";

        public DynamodbTestInterface build() throws URISyntaxException {
            final AwsCredentials awsCredentials = DefaultCredentialsProvider.create().resolveCredentials();

            return new AwsDynamodbTestInterface(DEFAULT_REGION, awsCredentials.accessKeyId(),
                    awsCredentials.secretAccessKey(), getSessionTokenIfPossible(awsCredentials));
        }

        private Optional<String> getSessionTokenIfPossible(final AwsCredentials awsCredentials) {
            if (awsCredentials instanceof AwsSessionCredentials) {
                return Optional.of(((AwsSessionCredentials) awsCredentials).sessionToken());
            } else {
                return Optional.empty();
            }
        }
    }
}
