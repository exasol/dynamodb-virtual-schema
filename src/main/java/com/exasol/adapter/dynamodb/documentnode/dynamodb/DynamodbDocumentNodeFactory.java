package com.exasol.adapter.dynamodb.documentnode.dynamodb;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.dynamodb.attributevalue.AttributeValueVisitor;
import com.exasol.dynamodb.attributevalue.AttributeValueWrapper;

/**
 * This class builds document nodes for a given {@link AttributeValue}. Weather a object- array- or value-node is built
 * depends on the type of the {@link AttributeValue}.
 */
public class DynamodbDocumentNodeFactory {

    /**
     * Builds a document node for a given {@link AttributeValue}.
     * 
     * @param value {@link AttributeValue} to wrap
     * @return object- array- or value-node
     */
    public DocumentNode buildDocumentNode(final AttributeValue value) {
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

        // TODO string set, ... should also be mapped as array
        @Override
        public void visitList(final List<AttributeValue> value) {
            this.converter = DynamodbArray::new;
        }

        @Override
        public void visitStringSet(final List<String> value) {
            this.converter = DynamodbStringSet::new;
        }

        @Override
        public void defaultVisit(final String typeName) {
            this.converter = DynamodbValue::new;
        }
    }
}
