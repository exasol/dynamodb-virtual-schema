package com.exasol.adapter.document.documentnode.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.exasol.dynamodb.attributevalue.AttributeValueQuickCreator;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class DynamodbMapTest {
    private static final String KEY = "key";
    private static final String VALUE = "value";
    private static final AttributeValue NESTED = AttributeValueQuickCreator.forString(VALUE);
    private static final AttributeValue MAP = AttributeValueQuickCreator.forMap(Map.of(KEY, NESTED));

    @Test
    void testGet() {
        final DynamodbMap dynamodbList = (DynamodbMap) new DynamodbDocumentNodeFactory().buildDocumentNode(MAP);
        final DynamodbString result = (DynamodbString) dynamodbList.get(KEY);
        assertThat(result.getValue(), equalTo(VALUE));
    }

    @Test
    void testGetKeyValueMap() {
        final DynamodbMap dynamodbList = (DynamodbMap) new DynamodbDocumentNodeFactory().buildDocumentNode(MAP);
        final DynamodbString result = (DynamodbString) dynamodbList.getKeyValueMap().get(KEY);
        assertThat(result.getValue(), equalTo(VALUE));
    }

    @Test
    void testSuccessfulHasKey() {
        final DynamodbMap dynamodbList = (DynamodbMap) new DynamodbDocumentNodeFactory().buildDocumentNode(MAP);
        assertThat(dynamodbList.hasKey(KEY), equalTo(true));
    }

    @Test
    void testNonExistingHasKey() {
        final DynamodbMap dynamodbList = (DynamodbMap) new DynamodbDocumentNodeFactory().buildDocumentNode(MAP);
        assertThat(dynamodbList.hasKey("unknownKey"), equalTo(false));
    }

    @Test
    void testVisitor() {
        final VisitationCheck visitor = new VisitationCheck();
        new DynamodbDocumentNodeFactory().buildDocumentNode(MAP).accept(visitor);
        assertThat(visitor.visited, equalTo(true));
    }

    private static class VisitationCheck implements IncompleteDynamodbNodeVisitor {
        boolean visited = false;

        @Override
        public void visit(final DynamodbMap map) {
            this.visited = true;
        }

        @Override
        public void defaultVisit(final String typeName) {
            throw new IllegalStateException("Should not be called");
        }
    }
}
