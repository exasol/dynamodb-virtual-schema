package com.exasol.adapter.dynamodb.documentnode.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.dynamodb.attributevalue.AttributeValueTestUtils;

/**
 * Tests for {@link DynamodbObject}
 */
public class DynamodbObjectTest {
    private static final String TEST_KEY = "key";
    private static final AttributeValue TEST_VALUE1 = AttributeValueTestUtils.forString("test1");

    private AttributeValue getTestObject() {
        final AttributeValue attributeValue = new AttributeValue();
        attributeValue.setM(Map.of(TEST_KEY, TEST_VALUE1));
        return attributeValue;
    }

    @Test
    void testSuccessfulHasKey() {
        final DynamodbObject dynamodbObject = new DynamodbObject(getTestObject());
        assertThat(dynamodbObject.hasKey(TEST_KEY), equalTo(true));
    }

    @Test
    void testUnsuccessfulHasKey() {
        final DynamodbObject dynamodbObject = new DynamodbObject(getTestObject());
        assertThat(dynamodbObject.hasKey("unknownKey"), equalTo(false));
    }

    @Test
    void testGet() {
        final DynamodbObject dynamodbObject = new DynamodbObject(getTestObject());
        final DynamodbValue value = (DynamodbValue) dynamodbObject.get(TEST_KEY);
        assertThat(value.getValue(), equalTo(TEST_VALUE1));
    }

    @Test
    void testGetKeyValueMap() {
        final DynamodbObject dynamodbObject = new DynamodbObject(getTestObject());
        final DynamodbValue value = (DynamodbValue) dynamodbObject.getKeyValueMap().get(TEST_KEY);
        assertThat(value.getValue(), equalTo(TEST_VALUE1));
    }
}
