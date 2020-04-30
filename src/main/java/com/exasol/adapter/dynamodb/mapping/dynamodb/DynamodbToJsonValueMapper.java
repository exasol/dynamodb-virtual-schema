package com.exasol.adapter.dynamodb.mapping.dynamodb;

import javax.json.JsonValue;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.ToJsonColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.ToJsonValueMapper;
import com.exasol.dynamodb.DynamodbValueToJsonConverter;

/**
 * This class represents {@link ToJsonValueMapper} for DynamoDB values.
 */
public class DynamodbToJsonValueMapper extends ToJsonValueMapper<DynamodbNodeVisitor> {

    /**
     * Creates an instance of {@link DynamodbToJsonValueMapper}.
     *
     * @param column {@link ToJsonColumnMappingDefinition}
     */
    public DynamodbToJsonValueMapper(final AbstractColumnMappingDefinition column) {
        super(column);
    }

    @Override
    protected String mapJsonValue(final DocumentNode<DynamodbNodeVisitor> dynamodbProperty) {
        final JsonValue json = new DynamodbValueToJsonConverter().convert(dynamodbProperty);
        return json.toString();
    }
}
