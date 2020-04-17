package com.exasol.adapter.dynamodb.dynamodbmetadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class DynamodbTableMetadata implements Serializable {
    private static final long serialVersionUID = 7898591572006230212L;
    private final DynamodbKey primaryKey;
    private final List<DynamodbKey> localIndexes;
    private final List<DynamodbKey> globalIndexes;

    public DynamodbTableMetadata(final DynamodbKey primaryKey, final List<DynamodbKey> localIndexes,
                                 final List<DynamodbKey> globalIndexes) {
        this.primaryKey = primaryKey;
        this.localIndexes = localIndexes;
        this.globalIndexes = globalIndexes;
    }

    public DynamodbKey getPrimaryKey() {
        return this.primaryKey;
    }

    public List<DynamodbKey> getLocalIndexes() {
        return this.localIndexes;
    }

    public List<DynamodbKey> getGlobalIndexes() {
        return this.globalIndexes;
    }

    public List<DynamodbKey> getAllIndexes() {
        final ArrayList<DynamodbKey> union = new ArrayList<>(this.localIndexes.size() + this.globalIndexes.size());
        union.addAll(this.localIndexes);
        union.addAll(this.globalIndexes);
        return union;
    }
}
