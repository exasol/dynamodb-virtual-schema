package com.exasol.adapter.dynamodb.mapping.dynamodb;

import java.math.BigDecimal;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNumber;
import com.exasol.adapter.dynamodb.mapping.PropertyToDecimalColumnMapping;
import com.exasol.adapter.dynamodb.mapping.PropertyToDecimalColumnValueExtractor;

/**
 * This class converts DynamoDB Numbers to DECIMAL columns.
 */
public class DynamodbPropertyToDecimalColumnValueExtractor
        extends PropertyToDecimalColumnValueExtractor<DynamodbNodeVisitor> {
    /**
     * Create an instance of {@link DynamodbPropertyToDecimalColumnValueExtractor}.
     *
     * @param column {@link PropertyToDecimalColumnMapping} defining the extraction
     */
    public DynamodbPropertyToDecimalColumnValueExtractor(final PropertyToDecimalColumnMapping column) {
        super(column);
    }

    @Override
    protected BigDecimal mapValueToDecimal(final DocumentNode<DynamodbNodeVisitor> documentValue) {
        if (!(documentValue instanceof DynamodbNumber)) {
            return null;
        } else {
            final DynamodbNumber dynamodbNumber = (DynamodbNumber) documentValue;
            return new BigDecimal(dynamodbNumber.getValue());
        }
    }
}
