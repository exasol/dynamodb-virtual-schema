package com.exasol.adapter.document.documentnode.dynamodb;

import java.util.HashMap;
import java.util.Map;

import com.exasol.adapter.document.documentnode.DocumentNode;
import com.exasol.adapter.document.documentnode.DocumentObject;
import com.exasol.errorreporting.ExaError;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * This class represents a DynamoDB map value.
 */
public class DynamodbMap implements DocumentObject {
    private final Map<String, AttributeValue> value;

    /**
     * Create an instance of {@link DynamodbMap}.
     *
     * @param value value to hold
     */
    public DynamodbMap(final Map<String, AttributeValue> value) {
        this.value = value;
    }

    @Override
    public Map<String, DocumentNode> getKeyValueMap() {
        final DynamodbDocumentNodeFactory nodeFactory = new DynamodbDocumentNodeFactory();
        final Map<String, DocumentNode> result = new HashMap<>();
        for (final Map.Entry<String, AttributeValue> entry : this.value.entrySet()) {
            if (result.put(entry.getKey(), nodeFactory.buildDocumentNode(entry.getValue())) != null) {
                throw new IllegalStateException(ExaError.messageBuilder("F-VS-DY-33")
                        .message("Invalid AttributeValue. The map had the same key twice.").ticketMitigation()
                        .toString());
            }
        }
        return result;
    }

    @Override
    public DocumentNode get(final String key) {
        return new DynamodbDocumentNodeFactory().buildDocumentNode(this.value.get(key));
    }

    @Override
    public boolean hasKey(final String key) {
        return this.value.containsKey(key);
    }

    Map<String, AttributeValue> getValue() {
        return this.value;
    }
}
