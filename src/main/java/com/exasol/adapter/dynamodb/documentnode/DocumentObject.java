package com.exasol.adapter.dynamodb.documentnode;

import java.util.Map;

/**
 * Interface for object document nodes.
 */
public interface DocumentObject extends DocumentNode {

    /**
     * Gives an map that represents this object. The values are wrapped as document nodes.
     * 
     * @return map representing this object
     */
    public Map<String, DocumentNode> getKeyValueMap();

    /**
     * Accesses a specific key of this object. The result is wrapped in a document node.
     * 
     * @param key The key that shall be accessed
     * @return result wrapped in a document node.
     */
    public DocumentNode get(String key);

    /**
     * Checks if this object contains a given key.
     * 
     * @param key key to check
     * @return {@code <TRUE>} if key is present
     */
    public boolean hasKey(String key);

    @Override
    public default void accept(final DocumentNodeVisitor visitor) {
        visitor.visit(this);
    }
}
