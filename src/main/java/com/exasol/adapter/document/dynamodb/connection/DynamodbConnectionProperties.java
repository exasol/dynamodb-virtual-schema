package com.exasol.adapter.document.dynamodb.connection;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class DynamodbConnectionProperties {
    @Builder.Default
    private final boolean useSsl = true;
    private final String awsAccessKeyId;
    private final String awsSecretAccessKey;
    private final String awsSessionToken;
    private final String awsRegion;
    private final String awsEndpointOverride;
}
