package com.exasol.adapter.dynamodb.mapping.dynamodb;

import javax.json.JsonValue;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.mapping.ToJsonPropertyToColumnMapping;
import com.exasol.adapter.dynamodb.mapping.ToJsonValueMapper;
import com.exasol.dynamodb.DynamodbValueToJsonConverter;

/**
 * This class represents {@link ToJsonValueMapper} for DynamoDB values.
 */
public class DynamodbToJsonValueMapper extends ToJsonValueMapper<DynamodbNodeVisitor> {

    /**
     * Creates an instance of {@link DynamodbToJsonValueMapper}.
     *
     * @param column {@link ToJsonPropertyToColumnMapping}
     */
    public DynamodbToJsonValueMapper(final ToJsonPropertyToColumnMapping column) {
        super(column);
    }

    @Override
    protected String mapJsonValue(final DocumentNode<DynamodbNodeVisitor> dynamodbProperty) {
        final JsonValue json = new DynamodbValueToJsonConverter().convert(dynamodbProperty);
        return json.toString();
    }
}
