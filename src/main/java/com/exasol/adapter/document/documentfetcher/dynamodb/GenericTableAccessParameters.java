package com.exasol.adapter.document.documentfetcher.dynamodb;

import java.io.Serializable;
import java.util.Map;
import java.util.stream.Collectors;

import com.exasol.adapter.document.documentnode.DocumentValue;
import com.exasol.adapter.document.documentnode.dynamodb.DynamodbNodeToAttributeValueConverter;
import com.exasol.adapter.document.documentnode.dynamodb.DynamodbNodeVisitor;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * This class groups the common parameters of Scan and Query document fetchers.
 */
public class GenericTableAccessParameters implements Serializable {
    private static final long serialVersionUID = -822082800908345226L;
    private final String tableName;
    private final Map<String, String> expressionAttributeNames;
    private final Map<String, DocumentValue<DynamodbNodeVisitor>> expressionAttributeValues;
    private final String filterExpression;
    private final String projectionExpression;

    /**
     * Create an instance of {@link GenericTableAccessParameters}.
     *
     * @param tableName                 name of the table to scan
     * @param expressionAttributeNames  map of placeholders for attribute names
     * @param expressionAttributeValues map of placeholders for attribute values
     * @param filterExpression          filter expression
     * @param projectionExpression      projection expression
     */
    GenericTableAccessParameters(final String tableName, final Map<String, String> expressionAttributeNames,
            final Map<String, DocumentValue<DynamodbNodeVisitor>> expressionAttributeValues,
            final String filterExpression, final String projectionExpression) {
        this.tableName = tableName;
        this.expressionAttributeNames = expressionAttributeNames;
        this.expressionAttributeValues = expressionAttributeValues;
        this.filterExpression = filterExpression;
        this.projectionExpression = projectionExpression;
    }

    /**
     * Get the name of the table to access.
     * 
     * @return name of the table
     */
    public String getTableName() {
        return this.tableName;
    }

    /**
     * Get the map of placeholders for attribute names.
     * 
     * @return map of placeholders for attribute names
     */
    public Map<String, String> getExpressionAttributeNames() {
        return this.expressionAttributeNames;
    }

    /**
     * Get if the map of placeholders for attribute names is not empty.
     * 
     * @return {@code true} if the map contains at least one entry
     */
    public boolean hasExpressionAttributeNames() {
        return !this.expressionAttributeNames.isEmpty();
    }

    /**
     * Get the map of placeholders for attribute values.
     * 
     * @return map of placeholders for attribute values
     */
    public Map<String, AttributeValue> getExpressionAttributeValues() {
        final DynamodbNodeToAttributeValueConverter converter = new DynamodbNodeToAttributeValueConverter();
        return this.expressionAttributeValues.entrySet().stream().collect(
                Collectors.toMap(Map.Entry::getKey, entry -> converter.convertToAttributeValue(entry.getValue())));
    }

    /**
     * Get if the map of placeholders for attribute names is not empty.
     *
     * @return {@code true} if the map contains at least one entry
     */
    public boolean hasExpressionAttributeValues() {
        return !this.expressionAttributeValues.isEmpty();
    }

    /**
     * Get the DynamoDB filter expression string.
     * 
     * @return filter expression string
     */
    public String getFilterExpression() {
        return this.filterExpression;
    }

    /**
     * Get if the filter expression is not empty.
     * 
     * @return true if not empty
     */
    public boolean hasFilterExpression() {
        return !this.filterExpression.isEmpty();
    }

    /**
     * Get the DynamoDB projection expression string.
     * 
     * @return projection expression string
     */
    public String getProjectionExpression() {
        return this.projectionExpression;
    }

    /**
     * Get if the projection expression is not empty.
     *
     * @return true if not empty
     */
    public boolean hasProjectionExpression() {
        return !this.projectionExpression.isEmpty();
    }

    /**
     * Create an builder for {@link @GenericTableAccessParameters}.
     *
     * @return builder for {@link @GenericTableAccessParameters}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link GenericTableAccessParameters}.
     */
    public static final class Builder {
        private String tableName;
        private Map<String, String> expressionAttributeNames;
        private Map<String, DocumentValue<DynamodbNodeVisitor>> expressionAttributeValues;
        private String filterExpression;
        private String projectionExpression;

        /**
         * Private constructor to hide the public default.
         */
        private Builder() {
            // empty on purpose
        }

        /**
         * Set the name of the table to scan.
         *
         * @param tableName name of the table
         * @return self
         */
        public Builder tableName(final String tableName) {
            this.tableName = tableName;
            return this;
        }

        /**
         * Set the placeholder map for attribute names.
         *
         * @param expressionAttributeNames placeholder map for attribute names
         * @return self
         */
        public Builder expressionAttributeNames(final Map<String, String> expressionAttributeNames) {
            this.expressionAttributeNames = expressionAttributeNames;
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
            this.expressionAttributeValues = expressionAttributeValues;
            return this;
        }

        /**
         * Set the filter expression
         *
         * @param filterExpression filter expression
         * @return self
         */
        public Builder filterExpression(final String filterExpression) {
            this.filterExpression = filterExpression;
            return this;
        }

        /**
         * Set the projection expression.
         *
         * @param projectionExpression projection expression
         * @return self
         */
        public Builder projectionExpression(final String projectionExpression) {
            this.projectionExpression = projectionExpression;
            return this;
        }

        public GenericTableAccessParameters build() {
            if(this.tableName == null){
                throw new IllegalStateException("tableName was not set but is a mandatory field.");
            }
            if(this.expressionAttributeNames == null){
                throw new IllegalStateException("expressionAttributeNames  was not set but is a mandatory field.");
            }
            if(this.expressionAttributeValues == null){
                throw new IllegalStateException("expressionAttributeValues  was not set but is a mandatory field.");
            }
            if(this.filterExpression == null){
                throw new IllegalStateException("filterExpression  was not set but is a mandatory field.");
            }
            if(this.projectionExpression == null){
                throw new IllegalStateException("projectionExpression  was not set but is a mandatory field.");
            }
            return new GenericTableAccessParameters(this.tableName, this.expressionAttributeNames,
                    this.expressionAttributeValues, this.filterExpression, this.projectionExpression);
        }
    }
}
