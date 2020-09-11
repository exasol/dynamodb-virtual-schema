package com.exasol.adapter.document.mapping.dynamodb;

import javax.json.JsonValue;

import com.exasol.adapter.document.documentnode.DocumentNode;
import com.exasol.adapter.document.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.document.mapping.PropertyToJsonColumnMapping;
import com.exasol.adapter.document.mapping.PropertyToJsonColumnValueExtractor;
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
