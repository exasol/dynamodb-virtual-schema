package com.exasol.adapter.dynamodb.mapping.dynamodb;

import javax.json.JsonValue;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.mapping.PropertyToJsonColumnMapping;
import com.exasol.adapter.dynamodb.mapping.PropertyToJsonColumnValueExtractor;
import com.exasol.dynamodb.DynamodbValueToJsonConverter;

/**
 * This class converts DynamoDB values to JSON strings in VARCHAR columns.
 */
public class DynamodbPropertyToJsonColumnValueExtractor
        extends PropertyToJsonColumnValueExtractor<DynamodbNodeVisitor> {

    /**
     * Create an instance of {@link DynamodbPropertyToJsonColumnValueExtractor}.
     *
     * @param column {@link PropertyToJsonColumnMapping}
     */
    public DynamodbPropertyToJsonColumnValueExtractor(final PropertyToJsonColumnMapping column) {
        super(column);
    }

    @Override
    protected String mapJsonValue(final DocumentNode<DynamodbNodeVisitor> dynamodbProperty) {
        final JsonValue json = DynamodbValueToJsonConverter.getInstance().convert(dynamodbProperty);
        return json.toString();
    }
}
