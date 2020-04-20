package com.exasol.adapter.dynamodb.documentnode.dynamodb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.dynamodb.documentnode.DocumentNode;

/**
 * This class builds document nodes for a given {@link AttributeValue}. Whether an object, an array, or a value-node is built
 * depends on the type of the {@link AttributeValue}.
 */
public class DynamodbDocumentNodeFactory {

    /**
     * Builds a document node for a given {@link AttributeValue}.
     * 
     * @param attributeValue {@link AttributeValue} to wrap
     * @return object, array, or value-node
     */
    public DocumentNode<DynamodbNodeVisitor> buildDocumentNode(final AttributeValue attributeValue) {
        if (attributeValue.getNULL() != null && attributeValue.getNULL()) {
            return new DynamodbNull();
        } else if (attributeValue.getS() != null) {
            return new DynamodbString(attributeValue.getS());
        } else if (attributeValue.getN() != null) {
            return new DynamodbNumber(attributeValue.getN());
        } else if (attributeValue.getB() != null) {
            return new DynamodbBinary(attributeValue.getB());
        } else if (attributeValue.getBOOL() != null) {
            return new DynamodbBoolean(attributeValue.getBOOL());
        } else if (attributeValue.getM() != null) {
            return new DynamodbObject(attributeValue.getM());
        } else if (attributeValue.getBS() != null) {
            return new DynamodbBinarySet(attributeValue.getBS());
        } else if (attributeValue.getL() != null) {
            return new DynamodbList(attributeValue.getL());
        } else if (attributeValue.getNS() != null) {
            return new DynamodbNumberSet(attributeValue.getNS());
        } else if (attributeValue.getSS() != null) {
            return new DynamodbStringSet(attributeValue.getSS());
        } else {
            throw new UnsupportedOperationException("Unsupported DynamoDB type.");
        }
    }
}
