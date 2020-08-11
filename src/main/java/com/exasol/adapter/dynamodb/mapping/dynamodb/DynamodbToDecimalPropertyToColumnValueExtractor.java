package com.exasol.adapter.dynamodb.mapping.dynamodb;

import java.math.BigDecimal;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNumber;
import com.exasol.adapter.dynamodb.mapping.ToDecimalPropertyToColumnMapping;
import com.exasol.adapter.dynamodb.mapping.ToDecimalPropertyToColumnValueExtractor;

/**
 * This class converts DynamoDB Numbers to DECIMAL columns.
 */
public class DynamodbToDecimalPropertyToColumnValueExtractor
        extends ToDecimalPropertyToColumnValueExtractor<DynamodbNodeVisitor> {
    /**
     * Create an instance of {@link DynamodbToDecimalPropertyToColumnValueExtractor}.
     *
     * @param column {@link ToDecimalPropertyToColumnMapping} defining the extraction
     */
    public DynamodbToDecimalPropertyToColumnValueExtractor(final ToDecimalPropertyToColumnMapping column) {
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
