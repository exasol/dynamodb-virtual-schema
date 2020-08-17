package com.exasol.adapter.document.documentnode.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.jupiter.api.Test;

import com.exasol.dynamodb.attributevalue.AttributeValueQuickCreator;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class DynamodbBooleanTest {
    private static final boolean TEST_VALUE = true;
    private static final AttributeValue ATTRIBUTE_VALUE = AttributeValueQuickCreator.forBoolean(TEST_VALUE);

    @Test
    void testCreation() {
        final DynamodbBoolean result = (DynamodbBoolean) new DynamodbDocumentNodeFactory()
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
        public void visit(final DynamodbBoolean bool) {
            this.visited = true;
        }

        @Override
        public void defaultVisit(final String typeName) {
            throw new IllegalStateException("Should not be called");
        }
    }
}
