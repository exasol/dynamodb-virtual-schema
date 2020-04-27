package com.exasol.adapter.dynamodb.documentnode.dynamodb;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentnode.DocumentValue;

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
        private final AttributeValue result = new AttributeValue();

        @Override
        public void visit(final DynamodbString string) {
            this.result.setS(string.getValue());
        }

        @Override
        public void visit(final DynamodbNumber number) {
            this.result.setN(number.getValue());
        }

        @Override
        public void visit(final DynamodbBinary binary) {
            this.result.setB(binary.getValue());
        }

        @Override
        public void visit(final DynamodbBoolean bool) {
            this.result.setBOOL(bool.getValue());
        }

        @Override
        public void visit(final DynamodbStringSet stringSet) {
            this.result.setSS(stringSet.getValue());
        }

        @Override
        public void visit(final DynamodbBinarySet binarySet) {
            final List<ByteBuffer> byteSet = binarySet.getValuesList().stream().map(DynamodbBinary::getValue)
                    .collect(Collectors.toList());
            this.result.setBS(byteSet);
        }

        @Override
        public void visit(final DynamodbNumberSet numberSet) {
            this.result.setNS(numberSet.getValue());
        }

        @Override
        public void visit(final DynamodbList list) {
            this.result.setL(list.getValue());
        }

        @Override
        public void visit(final DynamodbMap map) {
            this.result.setM(map.getValue());
        }

        @Override
        public void visit(final DynamodbNull nullValue) {
            this.result.setNULL(true);
        }

        private AttributeValue getResult() {
            return this.result;
        }
    }
}
