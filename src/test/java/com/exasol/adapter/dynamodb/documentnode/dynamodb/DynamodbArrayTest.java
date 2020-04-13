package com.exasol.adapter.dynamodb.documentnode.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.dynamodb.attributevalue.AttributeValueTestUtils;

/**
 * Tests for {@link DynamodbArray}
 */
public class DynamodbArrayTest {
    private static final AttributeValue TEST_VALUE1 = AttributeValueTestUtils.forString("test1");
    private static final AttributeValue TEST_VALUE2 = AttributeValueTestUtils.forString("test1");

    private AttributeValue getTestData() {
        final AttributeValue listAttributeValue = new AttributeValue();
        listAttributeValue.setL(List.of(TEST_VALUE1, TEST_VALUE2));
        return listAttributeValue;
    }

    @Test
    void testGetValueList() {
        final DynamodbArray dynamodbArray = new DynamodbArray(getTestData());
        final DynamodbValue value1 = (DynamodbValue) dynamodbArray.getValueList().get(0);
        final DynamodbValue value2 = (DynamodbValue) dynamodbArray.getValueList().get(1);
        assertAll(//
                () -> assertThat(value1.getValue(), equalTo(TEST_VALUE1)), //
                () -> assertThat(value2.getValue(), equalTo(TEST_VALUE2))//
        );
    }

    @Test
    void testGetValue() {
        final DynamodbArray dynamodbArray = new DynamodbArray(getTestData());
        final DynamodbValue value1 = (DynamodbValue) dynamodbArray.getValue(0);
        final DynamodbValue value2 = (DynamodbValue) dynamodbArray.getValue(1);
        assertAll(//
                () -> assertThat(value1.getValue(), equalTo(TEST_VALUE1)), //
                () -> assertThat(value2.getValue(), equalTo(TEST_VALUE2))//
        );
    }

}
