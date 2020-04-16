package com.exasol.adapter.dynamodb.documentnode.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

import java.nio.ByteBuffer;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.dynamodb.attributevalue.AttributeValueQuickCreator;

public class DynamodbBinarySetTest {
    private static final ByteBuffer BINARY_1 = ByteBuffer.wrap("test1".getBytes());
    private static final ByteBuffer BINARY_2 = ByteBuffer.wrap("test1".getBytes());
    private static final AttributeValue BINARY_SET = AttributeValueQuickCreator
            .forBinarySet(List.of(BINARY_1, BINARY_2));

    @Test
    void testGetValue() {
        final DynamodbBinarySet dynamodbList = (DynamodbBinarySet) new DynamodbDocumentNodeFactory()
                .buildDocumentNode(BINARY_SET);
        final DynamodbBinary result = (DynamodbBinary) dynamodbList.getValue(0);
        assertThat(result.getValue(), equalTo(BINARY_1));
    }

    @Test
    void testGetValues() {
        final DynamodbBinarySet dynamodbList = (DynamodbBinarySet) new DynamodbDocumentNodeFactory()
                .buildDocumentNode(BINARY_SET);
        final DynamodbBinary result1 = (DynamodbBinary) dynamodbList.getValuesList().get(0);
        final DynamodbBinary result2 = (DynamodbBinary) dynamodbList.getValuesList().get(1);
        assertThat(List.of(result1.getValue(), result2.getValue()), containsInAnyOrder(BINARY_1, BINARY_2));
    }

    @Test
    void testVisitor() {
        final VisitationCheck visitor = new VisitationCheck();
        new DynamodbDocumentNodeFactory().buildDocumentNode(BINARY_SET).accept(visitor);
        assertThat(visitor.wasVisited, equalTo(true));
    }

    private static class VisitationCheck implements DynamodbNodeVisitor {
        boolean wasVisited = false;

        @Override
        public void visit(final DynamodbBinarySet value) {
            this.wasVisited = true;
        }

        @Override
        public void defaultVisit(final String typeName) {
            throw new IllegalStateException("Should not be called");
        }
    }
}
