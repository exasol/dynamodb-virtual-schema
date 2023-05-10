package com.exasol.adapter.document.dynamodb.connection;

import java.util.Objects;

/**
 * Properties for a connection to DynamoDB
 */
public final class DynamodbConnectionProperties {

    private final boolean useSsl;
    private final String awsAccessKeyId;
    private final String awsSecretAccessKey;
    private final String awsSessionToken;
    private final String awsRegion;
    private final String awsEndpointOverride;

    private DynamodbConnectionProperties(final Builder builder) {
        this.useSsl = builder.useSsl;
        this.awsAccessKeyId = builder.awsAccessKeyId;
        this.awsSecretAccessKey = builder.awsSecretAccessKey;
        this.awsSessionToken = builder.awsSessionToken;
        this.awsRegion = builder.awsRegion;
        this.awsEndpointOverride = builder.awsEndpointOverride;
    }

    /**
     * @return {@code true} if TLS should be used for the connection
     */
    public boolean isUseSsl() {
        return this.useSsl;
    }

    /**
     * @return the awsAccessKeyId to use for the connection
     */
    public String getAwsAccessKeyId() {
        return this.awsAccessKeyId;
    }

    /**
     * @return the awsSecretAccessKey to use for the connection
     */
    public String getAwsSecretAccessKey() {
        return this.awsSecretAccessKey;
    }

    /**
     * @return the awsSessionToken to use for the connection
     */
    public String getAwsSessionToken() {
        return this.awsSessionToken;
    }

    /**
     * @return the awsRegion to use for the connection
     */
    public String getAwsRegion() {
        return this.awsRegion;
    }

    /**
     * @return the awsEndpointOverride to use for the connection
     */
    public String getAwsEndpointOverride() {
        return this.awsEndpointOverride;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.awsAccessKeyId, this.awsEndpointOverride, this.awsRegion, this.awsSecretAccessKey,
                this.awsSessionToken, this.useSsl);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final DynamodbConnectionProperties other = (DynamodbConnectionProperties) obj;
        return Objects.equals(this.awsAccessKeyId, other.awsAccessKeyId)
                && Objects.equals(this.awsEndpointOverride, other.awsEndpointOverride)
                && Objects.equals(this.awsRegion, other.awsRegion)
                && Objects.equals(this.awsSecretAccessKey, other.awsSecretAccessKey)
                && Objects.equals(this.awsSessionToken, other.awsSessionToken) && (this.useSsl == other.useSsl);
    }

    @Override
    public String toString() {
        return "DynamodbConnectionProperties [useSsl=" + this.useSsl + ", awsAccessKeyId=***"
                + ", awsSecretAccessKey=***" + ", awsSessionToken=***" + ", awsRegion=" + this.awsRegion
                + ", awsEndpointOverride=" + this.awsEndpointOverride + "]";
    }

    /**
     * Creates builder to build {@link DynamodbConnectionProperties}.
     *
     * @return created builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder to build {@link DynamodbConnectionProperties}.
     */
    public static final class Builder {
        private boolean useSsl = true;
        private String awsAccessKeyId;
        private String awsSecretAccessKey;
        private String awsSessionToken;
        private String awsRegion;
        private String awsEndpointOverride;

        private Builder() {
            // empty by intention
        }

        /**
         * Builder method for useSsl parameter.
         *
         * @param useSsl field to set
         * @return builder
         */
        public Builder useSsl(final boolean useSsl) {
            this.useSsl = useSsl;
            return this;
        }

        /**
         * Builder method for awsAccessKeyId parameter.
         *
         * @param awsAccessKeyId field to set
         * @return builder
         */
        public Builder awsAccessKeyId(final String awsAccessKeyId) {
            this.awsAccessKeyId = awsAccessKeyId;
            return this;
        }

        /**
         * Builder method for awsSecretAccessKey parameter.
         *
         * @param awsSecretAccessKey field to set
         * @return builder
         */
        public Builder awsSecretAccessKey(final String awsSecretAccessKey) {
            this.awsSecretAccessKey = awsSecretAccessKey;
            return this;
        }

        /**
         * Builder method for awsSessionToken parameter.
         *
         * @param awsSessionToken field to set
         * @return builder
         */
        public Builder awsSessionToken(final String awsSessionToken) {
            this.awsSessionToken = awsSessionToken;
            return this;
        }

        /**
         * Builder method for awsRegion parameter.
         *
         * @param awsRegion field to set
         * @return builder
         */
        public Builder awsRegion(final String awsRegion) {
            this.awsRegion = awsRegion;
            return this;
        }

        /**
         * Builder method for awsEndpointOverride parameter.
         *
         * @param awsEndpointOverride field to set
         * @return builder
         */
        public Builder awsEndpointOverride(final String awsEndpointOverride) {
            this.awsEndpointOverride = awsEndpointOverride;
            return this;
        }

        /**
         * Builder method of the builder.
         *
         * @return built class
         */
        public DynamodbConnectionProperties build() {
            return new DynamodbConnectionProperties(this);
        }
    }
}
