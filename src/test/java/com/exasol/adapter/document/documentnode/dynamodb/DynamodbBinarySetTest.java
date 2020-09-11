package com.exasol.adapter.document.documentnode.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.exasol.dynamodb.attributevalue.AttributeValueQuickCreator;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class DynamodbBinarySetTest {
    private static final SdkBytes BINARY_1 = SdkBytes.fromUtf8String("test1");
    private static final SdkBytes BINARY_2 = SdkBytes.fromUtf8String("test1");
    private static final AttributeValue BINARY_SET = AttributeValueQuickCreator
            .forBinarySet(List.of(BINARY_1, BINARY_2));

    @Test
    void testGetValue() {
        final DynamodbBinarySet dynamodbList = (DynamodbBinarySet) new DynamodbDocumentNodeFactory()
                .buildDocumentNode(BINARY_SET);
        final DynamodbBinary result = dynamodbList.getValue(0);
        assertThat(result.getValue(), equalTo(BINARY_1));
    }

    @Test
    void testGetValues() {
        final DynamodbBinarySet dynamodbList = (DynamodbBinarySet) new DynamodbDocumentNodeFactory()
                .buildDocumentNode(BINARY_SET);
        final DynamodbBinary result1 = dynamodbList.getValuesList().get(0);
        final DynamodbBinary result2 = dynamodbList.getValuesList().get(1);
        assertThat(List.of(result1.getValue(), result2.getValue()), containsInAnyOrder(BINARY_1, BINARY_2));
    }

    @Test
    void testVisitor() {
        final VisitationCheck visitor = new VisitationCheck();
        new DynamodbDocumentNodeFactory().buildDocumentNode(BINARY_SET).accept(visitor);
        assertThat(visitor.visited, equalTo(true));
    }

    private static class VisitationCheck implements IncompleteDynamodbNodeVisitor {
        boolean visited = false;

        @Override
        public void visit(final DynamodbBinarySet value) {
            this.visited = true;
        }

        @Override
        public void defaultVisit(final String typeName) {
            throw new IllegalStateException("Should not be called");
        }
    }
}
