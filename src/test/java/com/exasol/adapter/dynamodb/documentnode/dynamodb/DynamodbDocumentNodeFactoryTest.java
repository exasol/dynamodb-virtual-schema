package com.exasol.adapter.dynamodb.documentnode.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.dynamodb.documentnode.DocumentNode;

/**
 * Tests for {@link DynamodbDocumentNodeFactory}.
 */
public class DynamodbDocumentNodeFactoryTest {

    @Test
    void testCreateObject() {
        final AttributeValue attributeValue = new AttributeValue();
        attributeValue.setM(Map.of("key", new AttributeValue()));
        final DocumentNode documentNode = new DynamodbDocumentNodeFactory().buildDocumentNode(attributeValue);
        assertThat(documentNode, instanceOf(DynamodbObject.class));
    }

    @Test
    void testCreateArray() {
        final AttributeValue attributeValue = new AttributeValue();
        attributeValue.setL(List.of(new AttributeValue()));
        final DocumentNode documentNode = new DynamodbDocumentNodeFactory().buildDocumentNode(attributeValue);
        assertThat(documentNode, instanceOf(DynamodbArray.class));
    }

    @Test
    void testCreateValue() {
        final AttributeValue attributeValue = new AttributeValue();
        attributeValue.setS("test");
        final DocumentNode documentNode = new DynamodbDocumentNodeFactory().buildDocumentNode(attributeValue);
        assertThat(documentNode, instanceOf(DynamodbValue.class));
    }
}
