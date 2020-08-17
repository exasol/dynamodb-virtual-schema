package com.exasol.adapter.document.documentnode.dynamodb;

import java.util.List;
import java.util.stream.Collectors;

import com.exasol.adapter.document.documentnode.DocumentNode;
import com.exasol.adapter.document.documentnode.DocumentValue;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * This class converts a {@link DocumentValue <DynamodbNodeVisitor>} into a DynamoDB {@link AttributeValue}.
 */
public class DynamodbNodeToAttributeValueConverter {

    /**
     * Converts a {@link DocumentValue <DynamodbNodeVisitor>} into a DynamoDB {@link AttributeValue}.
     * 
     * @param node document node to convert
     * @return {@link AttributeValue}
     */
    public AttributeValue convertToAttributeValue(final DocumentNode<DynamodbNodeVisitor> node) {
        final Visitor visitor = new Visitor();
        node.accept(visitor);
        return visitor.getResult();
    }

    private static class Visitor implements DynamodbNodeVisitor {
        private AttributeValue result;

        @Override
        public void visit(final DynamodbString string) {
            this.result = AttributeValue.builder().s(string.getValue()).build();
        }

        @Override
        public void visit(final DynamodbNumber number) {
            this.result = AttributeValue.builder().n(number.getValue()).build();
        }

        @Override
        public void visit(final DynamodbBinary binary) {
            this.result = AttributeValue.builder().b(binary.getValue()).build();
        }

        @Override
        public void visit(final DynamodbBoolean bool) {
            this.result = AttributeValue.builder().bool(bool.getValue()).build();
        }

        @Override
        public void visit(final DynamodbStringSet stringSet) {
            this.result = AttributeValue.builder().ss(stringSet.getValue()).build();
        }

        @Override
        public void visit(final DynamodbBinarySet binarySet) {
            final List<SdkBytes> byteSet = binarySet.getValuesList().stream().map(DynamodbBinary::getValue)
                    .collect(Collectors.toList());
            this.result = AttributeValue.builder().bs(byteSet).build();
        }

        @Override
        public void visit(final DynamodbNumberSet numberSet) {
            this.result = AttributeValue.builder().ns(numberSet.getValue()).build();
        }

        @Override
        public void visit(final DynamodbList list) {
            this.result = AttributeValue.builder().l(list.getValue()).build();
        }

        @Override
        public void visit(final DynamodbMap map) {
            this.result = AttributeValue.builder().m(map.getValue()).build();
        }

        @Override
        public void visit(final DynamodbNull nullValue) {
            this.result = AttributeValue.builder().nul(true).build();
        }

        private AttributeValue getResult() {
            return this.result;
        }
    }
}
