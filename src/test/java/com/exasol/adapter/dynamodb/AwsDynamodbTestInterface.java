package com.exasol.adapter.dynamodb;

import java.util.Optional;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;

public class AwsDynamodbTestInterface extends DynamodbTestInterface {

    private AwsDynamodbTestInterface(final String dynamoUrl, final String user, final String pass,
            final Optional<String> sessionToken) {
        super(dynamoUrl, user, pass, sessionToken);
    }

    @Override
    public void teardown() {

    }

    public static class Builder {
        private static final String DEFAULT_REGION = "aws:eu-central-1";

        public DynamodbTestInterface build() {
            final AWSCredentials awsCredentials = DefaultAWSCredentialsProviderChain.getInstance().getCredentials();
            return new AwsDynamodbTestInterface(DEFAULT_REGION, awsCredentials.getAWSAccessKeyId(),
                    awsCredentials.getAWSSecretKey(), getSessionTokenIfPossible(awsCredentials));
        }

        private Optional<String> getSessionTokenIfPossible(final AWSCredentials awsCredentials) {
            if (awsCredentials instanceof AWSSessionCredentials) {
                final AWSSessionCredentials sessionCredentials = (AWSSessionCredentials) awsCredentials;
                return Optional.of(((AWSSessionCredentials) awsCredentials).getSessionToken());
            } else {
                return Optional.empty();
            }
        }
    }
}
