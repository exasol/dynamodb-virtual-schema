package com.exasol.adapter.dynamodb.keyinfo;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;

import java.util.List;

public class DynamodbKeyInfo {
    private final DocumentPathExpression partitionKey;
    private final DocumentPathExpression searchKey;
    private final List<DocumentPathExpression> localIndexes;
    private final List<DynamodbGlobalIndex> globalIndexes;

    public DynamodbKeyInfo(final DocumentPathExpression partitionKey, final DocumentPathExpression searchKey, final List<DocumentPathExpression> localIndexes, final List<DynamodbGlobalIndex> globalIndexes) {
        this.partitionKey = partitionKey;
        this.searchKey = searchKey;
        this.localIndexes = localIndexes;
        this.globalIndexes = globalIndexes;
    }

    public DocumentPathExpression getPartitionKey() {
        return this.partitionKey;
    }

    public DocumentPathExpression getSearchKey() {
        return this.searchKey;
    }

    public List<DocumentPathExpression> getLocalIndexes() {
        return this.localIndexes;
    }

    public List<DynamodbGlobalIndex> getGlobalIndexes() {
        return this.globalIndexes;
    }
}
