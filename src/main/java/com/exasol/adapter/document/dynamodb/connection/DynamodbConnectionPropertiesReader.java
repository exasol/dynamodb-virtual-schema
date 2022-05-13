package com.exasol.adapter.document.dynamodb.connection;

import com.exasol.adapter.document.connection.ConnectionPropertiesReader;

/**
 * Reader for {@link DynamodbConnectionProperties}.
 */
public class DynamodbConnectionPropertiesReader {
    public DynamodbConnectionProperties read(final ConnectionPropertiesReader reader) {
        return DynamodbConnectionProperties.builder()//
                .useSsl(reader.readBooleanWithDefault("useSsl", true))//
                .awsAccessKeyId(reader.readRequiredString("awsAccessKeyId"))
                .awsSecretAccessKey(reader.readRequiredString("awsSecretAccessKey"))
                .awsRegion(reader.readRequiredString("awsRegion"))
                .awsEndpointOverride(reader.readString("awsEndpointOverride").orElse(null))
                .awsSessionToken(reader.readString("awsSessionToken").orElse(null))//
                .build();
    }
}
