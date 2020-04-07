package com.exasol.adapter.dynamodb.documentnode.dynamodb;

import java.nio.ByteBuffer;
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
        public void visitString(final String value) {
            this.converter = DynamodbValue::new;
        }

        @Override
        public void visitNumber(final String value) {
            this.converter = DynamodbValue::new;
        }

        @Override
        public void visitBinary(final ByteBuffer value) {
            this.converter = DynamodbValue::new;
        }

        @Override
        public void visitBoolean(final boolean value) {
            this.converter = DynamodbValue::new;
        }

        @Override
        public void visitMap(final Map<String, AttributeValue> value) {
            this.converter = DynamodbObject::new;
        }

        @Override
        public void visitByteSet(final List<ByteBuffer> value) {
            this.converter = DynamodbValue::new;
        }

        @Override
        public void visitList(final List<AttributeValue> value) {
            this.converter = DynamodbArray::new;
        }

        @Override
        public void visitNumberSet(final List<String> value) {
            this.converter = DynamodbValue::new;
        }

        @Override
        public void visitStringSet(final List<String> value) {
            this.converter = DynamodbValue::new;
        }

        @Override
        public void visitNull() {
            this.converter = DynamodbValue::new;
        }
    }

}