package com.exasol.adapter.document.documentnode.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.exasol.dynamodb.attributevalue.AttributeValueQuickCreator;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class DynamodbBinaryTest {
    private static final SdkBytes TEST_VALUE = SdkBytes.fromUtf8String("test");
    private static final AttributeValue ATTRIBUTE_VALUE = AttributeValueQuickCreator.forBinary(TEST_VALUE);

    @Test
    void testCreation() {
        final DynamodbBinary result = (DynamodbBinary) new DynamodbDocumentNodeFactory()
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
        public void visit(final DynamodbBinary value) {
            this.visited = true;
        }

        @Override
        public void defaultVisit(final String typeName) {
            Assertions.fail("Wrong visit method was called.");
        }
    }
}
