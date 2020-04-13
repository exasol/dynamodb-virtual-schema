package com.exasol.adapter.dynamodb.documentnode.dynamodb;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.dynamodb.attributevalue.AttributeValueVisitor;
import com.exasol.dynamodb.attributevalue.AttributeValueWrapper;

public class DynamodbDocumentNodeFactory {
    DocumentNode buildDocumentNode(final AttributeValue value) {
        final AttributeValueWrapper attributeValueWrapper = new AttributeValueWrapper(value);
        final Visitor visitor = new Visitor();
        attributeValueWrapper.accept(visitor);
        return visitor.converter.apply(value);
    }

    private static class Visitor implements AttributeValueVisitor {
        private Function<AttributeValue, DocumentNode> converter;

        @Override
        public void visitMap(final Map<String, AttributeValue> value) {
            this.converter = DynamodbObject::new;
        }

        @Override
        public void visitList(final List<AttributeValue> value) {
            this.converter = DynamodbArray::new;
        }

        @Override
        public void defaultVisit(final String typeName) {
            this.converter = DynamodbValue::new;
        }
    }
}
