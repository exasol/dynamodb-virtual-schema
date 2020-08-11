package com.exasol.adapter.dynamodb.mapping.dynamodb;

import javax.json.JsonValue;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.mapping.ToJsonPropertyToColumnMapping;
import com.exasol.adapter.dynamodb.mapping.ToJsonPropertyToColumnValueExtractor;
import com.exasol.dynamodb.DynamodbValueToJsonConverter;

/**
 * This class converts DynamoDB values to JSON strings in VARCHAR columns.
 */
public class DynamodbToJsonPropertyToColumnValueExtractor
        extends ToJsonPropertyToColumnValueExtractor<DynamodbNodeVisitor> {

    /**
     * Create an instance of {@link DynamodbToJsonPropertyToColumnValueExtractor}.
     *
     * @param column {@link ToJsonPropertyToColumnMapping}
     */
    public DynamodbToJsonPropertyToColumnValueExtractor(final ToJsonPropertyToColumnMapping column) {
        super(column);
    }

    @Override
    protected String mapJsonValue(final DocumentNode<DynamodbNodeVisitor> dynamodbProperty) {
        final JsonValue json = DynamodbValueToJsonConverter.getInstance().convert(dynamodbProperty);
        return json.toString();
    }
}
