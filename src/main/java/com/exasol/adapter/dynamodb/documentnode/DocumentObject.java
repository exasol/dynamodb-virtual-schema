package com.exasol.adapter.dynamodb.documentnode;

import java.util.Map;

/**
 * Interface for object document nodes.
 */
public interface DocumentObject extends DocumentNode {
    public Map<String, DocumentNode> getKeyValueMap();

    public DocumentNode get(String key);

    public boolean hasKey(String key);

    @Override
    public default void accept(final DocumentNodeVisitor visitor) {
        visitor.visit(this);
    }
}
