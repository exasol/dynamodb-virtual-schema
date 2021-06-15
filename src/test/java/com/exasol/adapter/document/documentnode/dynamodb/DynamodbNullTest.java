package com.exasol.adapter.document.documentnode.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.document.documentnode.DocumentNode;
import com.exasol.dynamodb.attributevalue.AttributeValueQuickCreator;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class DynamodbNullTest {
    private static final AttributeValue ATTRIBUTE_VALUE = AttributeValueQuickCreator.forNull();

    @Test
    void testCreation() {
        final DocumentNode<DynamodbNodeVisitor> result = new DynamodbDocumentNodeFactory()
                .buildDocumentNode(ATTRIBUTE_VALUE);
        assertThat(result, instanceOf(DynamodbNull.class));
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
        public void visit(final DynamodbNull value) {
            this.visited = true;
        }

        @Override
        public void defaultVisit(final String typeName) {
            Assertions.fail("Wrong visit method was called.");
        }
    }
}
