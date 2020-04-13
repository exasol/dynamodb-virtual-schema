package com.exasol.adapter.dynamodb.documentnode.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

import java.nio.ByteBuffer;
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
    void testCreateArrayFromList() {
        final AttributeValue attributeValue = new AttributeValue();
        attributeValue.setL(List.of(new AttributeValue()));
        final DocumentNode documentNode = new DynamodbDocumentNodeFactory().buildDocumentNode(attributeValue);
        assertThat(documentNode, instanceOf(DynamodbArray.class));
    }

    @Test
    void testCreateArrayFromStringSet() {
        final AttributeValue attributeValue = new AttributeValue();
        attributeValue.setSS(List.of(""));
        final DocumentNode documentNode = new DynamodbDocumentNodeFactory().buildDocumentNode(attributeValue);
        assertThat(documentNode, instanceOf(DynamodbArray.class));
    }

    @Test
    void testCreateArrayFromNumberSet() {
        final AttributeValue attributeValue = new AttributeValue();
        attributeValue.setNS(List.of(""));
        final DocumentNode documentNode = new DynamodbDocumentNodeFactory().buildDocumentNode(attributeValue);
        assertThat(documentNode, instanceOf(DynamodbArray.class));
    }

    @Test
    void testCreateArrayFromBinarySet() {
        final AttributeValue attributeValue = new AttributeValue();
        attributeValue.setBS(List.of(ByteBuffer.wrap("".getBytes())));
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
