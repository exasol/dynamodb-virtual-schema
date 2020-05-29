package com.exasol.adapter.dynamodb;

import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.mapping.PropertyToColumnValueExtractorFactory;
import com.exasol.adapter.dynamodb.mapping.dynamodb.DynamodbPropertyToColumnValueExtractorFactory;

/**
 * This class is the UDF implementation for DynamoDB. All program logic is done in the abstract base.
 */
class DynamodbUdf extends AbstractUdf<DynamodbNodeVisitor> {
    @Override
    protected PropertyToColumnValueExtractorFactory<DynamodbNodeVisitor> getDocumentVisitorTypePropertyToColumnValueExtractorFactory() {
        return new DynamodbPropertyToColumnValueExtractorFactory();
    }
}
