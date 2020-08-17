package com.exasol.adapter.document.dynamodb;

import com.exasol.adapter.document.AbstractUdf;
import com.exasol.adapter.document.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.document.mapping.PropertyToColumnValueExtractorFactory;
import com.exasol.adapter.document.mapping.dynamodb.DynamodbPropertyToColumnValueExtractorFactory;

/**
 * This class is the UDF implementation for DynamoDB. All program logic is done in the abstract base.
 */
public class DynamodbUdf extends AbstractUdf<DynamodbNodeVisitor> {
    @Override
    protected PropertyToColumnValueExtractorFactory<DynamodbNodeVisitor> getValueExtractorFactory() {
        return new DynamodbPropertyToColumnValueExtractorFactory();
    }
}
