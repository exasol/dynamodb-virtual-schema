package com.exasol.adapter.dynamodb.documentnode.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.dynamodb.attributevalue.AttributeValueQuickCreator;

class DynamodbNodeToAttributeValueConverterTest {
    private final static DynamodbNodeToAttributeValueConverter CONVERTER = new DynamodbNodeToAttributeValueConverter();

    @Test
    void testConvertString() {
        final String testValue = "test";
        final AttributeValue result = CONVERTER.convertToAttributeValue(new DynamodbString(testValue));
        assertThat(result.getS(), equalTo(testValue));
    }

    @Test
    void testConvertNumber() {
        final String testValue = "10";
        final AttributeValue result = CONVERTER.convertToAttributeValue(new DynamodbNumber(testValue));
        assertThat(result.getN(), equalTo(testValue));
    }

    @Test
    void testConvertBinary() {
        final ByteBuffer testValue = ByteBuffer.wrap("test".getBytes());
        final AttributeValue result = CONVERTER.convertToAttributeValue(new DynamodbBinary(testValue));
        assertThat(result.getB(), equalTo(testValue));
    }

    @Test
    void testConvertBoolean() {
        final AttributeValue result = CONVERTER.convertToAttributeValue(new DynamodbBoolean(true));
        assertThat(result.getBOOL(), equalTo(true));
    }

    @Test
    void testConvertStringSet() {
        final String testValue = "test";
        final AttributeValue result = CONVERTER.convertToAttributeValue(new DynamodbStringSet(List.of(testValue)));
        assertThat(result.getSS(), containsInAnyOrder(testValue));
    }

    @Test
    void testConvertNumberSet() {
        final String testValue = "123";
        final AttributeValue result = CONVERTER.convertToAttributeValue(new DynamodbNumberSet(List.of(testValue)));
        assertThat(result.getNS(), containsInAnyOrder(testValue));
    }

    @Test
    void testConvertBinarySet() {
        final ByteBuffer testValue = ByteBuffer.wrap("test".getBytes());
        final AttributeValue result = CONVERTER.convertToAttributeValue(new DynamodbBinarySet(List.of(testValue)));
        assertThat(result.getBS(), containsInAnyOrder(testValue));
    }

    @Test
    void testConvertList() {
        final AttributeValue testValue = AttributeValueQuickCreator.forString("test");
        final AttributeValue result = CONVERTER.convertToAttributeValue(new DynamodbList(List.of(testValue)));
        assertThat(result.getL(), containsInAnyOrder(testValue));
    }

    @Test
    void testConvertMap() {
        final Map<String, AttributeValue> testValue = Map.of("key", AttributeValueQuickCreator.forString("test"));
        final AttributeValue result = CONVERTER.convertToAttributeValue(new DynamodbMap(testValue));
        assertThat(result.getM(), equalTo(testValue));
    }

    @Test
    void testConvertNull() {
        final AttributeValue result = CONVERTER.convertToAttributeValue(new DynamodbNull());
        assertThat(result.getNULL(), equalTo(true));
    }
}