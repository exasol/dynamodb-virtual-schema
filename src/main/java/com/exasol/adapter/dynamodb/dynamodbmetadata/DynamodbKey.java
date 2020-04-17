package com.exasol.adapter.dynamodb.dynamodbmetadata;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;

import java.io.Serializable;

public class DynamodbKey implements Serializable {
    private static final long serialVersionUID = 4048033058983909214L;
    private final DocumentPathExpression partitionKey;
    private final DocumentPathExpression sortKey;

    public DynamodbKey(final DocumentPathExpression partitionKey, final DocumentPathExpression sortKey) {
        this.partitionKey = partitionKey;
        this.sortKey = sortKey;
    }

    public DocumentPathExpression getPartitionKey() {
        return this.partitionKey;
    }

    public DocumentPathExpression getSortKey() {
        return this.sortKey;
    }

    public boolean hasSortKey(){
        return this.sortKey != null;
    }
}
