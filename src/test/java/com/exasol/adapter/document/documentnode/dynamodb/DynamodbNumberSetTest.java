package com.exasol.adapter.document.documentnode.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.exasol.dynamodb.attributevalue.AttributeValueQuickCreator;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class DynamodbNumberSetTest {
    private static final String NUMBER_1 = "123";
    private static final String NUMBER_2 = "456";
    private static final AttributeValue NUMBER_SET = AttributeValueQuickCreator
            .forNumberSet(List.of(NUMBER_1, NUMBER_2));

    @Test
    void testGetValue() {
        final DynamodbNumberSet dynamodbList = (DynamodbNumberSet) new DynamodbDocumentNodeFactory()
                .buildDocumentNode(NUMBER_SET);
        final DynamodbNumber result = dynamodbList.getValue(0);
        assertThat(result.getValue(), equalTo(NUMBER_1));
    }

    @Test
    void testGetValues() {
        final DynamodbNumberSet dynamodbList = (DynamodbNumberSet) new DynamodbDocumentNodeFactory()
                .buildDocumentNode(NUMBER_SET);
        final DynamodbNumber result1 = dynamodbList.getValuesList().get(0);
        final DynamodbNumber result2 = dynamodbList.getValuesList().get(1);
        assertThat(List.of(result1.getValue(), result2.getValue()), containsInAnyOrder(NUMBER_1, NUMBER_2));
    }

    @Test
    void testVisitor() {
        final VisitationCheck visitor = new VisitationCheck();
        new DynamodbDocumentNodeFactory().buildDocumentNode(NUMBER_SET).accept(visitor);
        assertThat(visitor.visited, equalTo(true));
    }

    private static class VisitationCheck implements IncompleteDynamodbNodeVisitor {
        boolean visited = false;

        @Override
        public void visit(final DynamodbNumberSet value) {
            this.visited = true;
        }

        @Override
        public void defaultVisit(final String typeName) {
            throw new IllegalStateException("Should not be called");
        }
    }
}
