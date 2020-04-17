package com.exasol.adapter.dynamodb.keyinfo;

import java.util.List;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;

public class DynamodbKeyInfoFactory {
    public void forDynamodbTable(final AmazonDynamoDB connection, final String tableName) {
        final DescribeTableResult describeTableResult = connection.describeTable(tableName);
        final List<KeySchemaElement> keySchema = describeTableResult.getTable().getKeySchema();
        String partitionKey;
        String sortKey;
        for (final KeySchemaElement keySchemaElement : keySchema) {
            if (keySchemaElement.getKeyType().equals("HASH")) {
                partitionKey = keySchemaElement.getAttributeName();
            } else {
                sortKey = keySchemaElement.getAttributeName();
            }
        }
    }
}
