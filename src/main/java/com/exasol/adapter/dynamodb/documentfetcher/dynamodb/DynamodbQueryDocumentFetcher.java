package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import java.util.Map;
import java.util.stream.Stream;

import com.exasol.adapter.dynamodb.documentnode.DocumentValue;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;

/**
 * This class represents a DynamoDB {@code QUERY} operation.
 */
public class DynamodbQueryDocumentFetcher extends AbstractDynamodbDocumentFetcher {
    private static final long serialVersionUID = -810868396675247321L;//

    private final GenericTableAccessParameters genericParameters;
    private final String indexName;
    private final String keyConditionExpression;

    /**
     * Create an a {@link DynamodbQueryDocumentFetcher}.
     *
     * @param genericParameters      generic parameters (not query specific)
     * @param indexName              name of the index to scan
     * @param keyConditionExpression condition expression for key columns
     */
    private DynamodbQueryDocumentFetcher(final GenericTableAccessParameters genericParameters, final String indexName,
            final String keyConditionExpression) {
        this.genericParameters = genericParameters;
        this.indexName = indexName;
        this.keyConditionExpression = keyConditionExpression;
    }

    static Builder builder() {
        return new Builder();
    }

    QueryRequest getQueryRequest() {
        final QueryRequest.Builder builder = QueryRequest.builder().tableName(this.genericParameters.getTableName());
        if (this.genericParameters.hasExpressionAttributeNames()) {
            builder.expressionAttributeNames(this.genericParameters.getExpressionAttributeNames());
        }
        if (this.genericParameters.hasFilterExpression()) {
            builder.filterExpression(this.genericParameters.getFilterExpression());
        }
        if (this.genericParameters.hasExpressionAttributeValues()) {
            builder.expressionAttributeValues(this.genericParameters.getExpressionAttributeValues());
        }
        if (this.genericParameters.hasProjectionExpression()) {
            builder.projectionExpression(this.genericParameters.getProjectionExpression());
        }
        if (this.indexName != null) {
            builder.indexName(this.indexName);
        }
        builder.keyConditionExpression(this.keyConditionExpression);
        return builder.build();
    }

    @Override
    public Stream<Map<String, AttributeValue>> run(final DynamoDbClient client) {
        return client.query(this.getQueryRequest()).items().stream();
    }

    /**
     * Builder for {@link DynamodbQueryDocumentFetcher}.
     */
    public static final class Builder {
        private final GenericTableAccessParameters.Builder genericParametersBuilder;
        private String indexName;
        private String keyConditionExpression;

        private Builder() {
            this.genericParametersBuilder = GenericTableAccessParameters.builder();
        }

        /**
         * Set the name of the table to scan.
         *
         * @param tableName name of the table
         * @return self
         */
        public Builder tableName(final String tableName) {
            this.genericParametersBuilder.tableName(tableName);
            return this;
        }

        /**
         * Set the placeholder map for attribute names.
         *
         * @param expressionAttributeNames placeholder map for attribute names
         * @return self
         */
        public Builder expressionAttributeNames(final Map<String, String> expressionAttributeNames) {
            this.genericParametersBuilder.expressionAttributeNames(expressionAttributeNames);
            return this;
        }

        /**
         * Set the placeholder map for attribute values.
         *
         * @param expressionAttributeValues placeholder map for attribute values
         * @return self
         */
        public Builder expressionAttributeValues(
                final Map<String, DocumentValue<DynamodbNodeVisitor>> expressionAttributeValues) {
            this.genericParametersBuilder.expressionAttributeValues(expressionAttributeValues);
            return this;
        }

        /**
         * Set the filter expression
         *
         * @param filterExpression filter expression
         * @return self
         */
        public Builder filterExpression(final String filterExpression) {
            this.genericParametersBuilder.filterExpression(filterExpression);
            return this;
        }

        /**
         * Set the projection expression.
         *
         * @param projectionExpression projection expression
         * @return self
         */
        public Builder projectionExpression(final String projectionExpression) {
            this.genericParametersBuilder.projectionExpression(projectionExpression);
            return this;
        }

        /**
         * Set the name of the index to query.
         *
         * @param indexName name of the index
         * @return self
         */
        public Builder indexName(final String indexName) {
            this.indexName = indexName;
            return this;
        }

        /**
         * Set the condition expression for key columns.
         *
         * @param keyConditionExpression condition expression string
         * @return self
         */
        public Builder keyConditionExpression(final String keyConditionExpression) {
            this.keyConditionExpression = keyConditionExpression;
            return this;
        }

        /**
         * Build the {@link DynamodbQueryDocumentFetcher}.
         *
         * @return {@link DynamodbQueryDocumentFetcher}
         */
        public DynamodbQueryDocumentFetcher build() {
            if(this.indexName == null){
                throw new IllegalStateException("indexName was not set but is a mandatory field.");
            }
            if(this.keyConditionExpression == null){
                throw new IllegalStateException("keyConditionExpression was not set but is a mandatory field.");
            }
            return new DynamodbQueryDocumentFetcher(this.genericParametersBuilder.build(), this.indexName,
                    this.keyConditionExpression);
        }
    }
}
