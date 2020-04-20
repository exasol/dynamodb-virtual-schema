package com.exasol.adapter.dynamodb.mapping.tojsonmapping.dynamodb;

import javax.json.JsonValue;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.tojsonmapping.ToJsonColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.tojsonmapping.ToJsonValueMapper;
import com.exasol.dynamodb.DynamodbValueToJsonConverter;

public class DynamodbToJsonValueMapper extends ToJsonValueMapper<DynamodbNodeVisitor> {

    /**
     * Creates an instance of {@link DynamodbToJsonValueMapper}
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
