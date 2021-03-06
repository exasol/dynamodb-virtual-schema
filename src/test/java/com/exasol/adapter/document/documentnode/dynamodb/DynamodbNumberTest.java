package com.exasol.adapter.document.documentnode.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.jupiter.api.Test;

import com.exasol.dynamodb.attributevalue.AttributeValueQuickCreator;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class DynamodbNumberTest {
    private static final String TEST_VALUE = "123";
    private static final AttributeValue ATTRIBUTE_VALUE = AttributeValueQuickCreator.forNumber(TEST_VALUE);

    @Test
    void testCreation() {
        final DynamodbNumber result = (DynamodbNumber) new DynamodbDocumentNodeFactory()
                .buildDocumentNode(ATTRIBUTE_VALUE);
        assertThat(result.getValue(), equalTo(TEST_VALUE));
    }

    @Test
    void testVisitor() {
        final VisitationCheck visitor = new VisitationCheck();
        new DynamodbDocumentNodeFactory().buildDocumentNode(ATTRIBUTE_VALUE).accept(visitor);
        assertThat(visitor.visited, equalTo(true));
    }

    private static class VisitationCheck implements IncompleteDynamodbNodeVisitor {
        boolean visited = false;

        @Override
        public void visit(final DynamodbNumber number) {
            this.visited = true;
        }

        @Override
        public void defaultVisit(final String typeName) {
            throw new IllegalStateException("Should not be called");
        }
    }
}
