package com.exasol.adapter.dynamodb.documentnode.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.exasol.dynamodb.attributevalue.AttributeValueQuickCreator;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class DynamodbNodeToAttributeValueConverterTest {
    private final static DynamodbNodeToAttributeValueConverter CONVERTER = new DynamodbNodeToAttributeValueConverter();

    @Test
    void testConvertString() {
        final String testValue = "test";
        final AttributeValue result = CONVERTER.convertToAttributeValue(new DynamodbString(testValue));
        assertThat(result.s(), equalTo(testValue));
    }

    @Test
    void testConvertNumber() {
        final String testValue = "10";
        final AttributeValue result = CONVERTER.convertToAttributeValue(new DynamodbNumber(testValue));
        assertThat(result.n(), equalTo(testValue));
    }

    @Test
    void testConvertBinary() {
        final SdkBytes testValue = SdkBytes.fromUtf8String("test");
        final AttributeValue result = CONVERTER.convertToAttributeValue(new DynamodbBinary(testValue));
        assertThat(result.b(), equalTo(testValue));
    }

    @Test
    void testConvertBoolean() {
        final AttributeValue result = CONVERTER.convertToAttributeValue(new DynamodbBoolean(true));
        assertThat(result.bool(), equalTo(true));
    }

    @Test
    void testConvertStringSet() {
        final String testValue = "test";
        final AttributeValue result = CONVERTER.convertToAttributeValue(new DynamodbStringSet(List.of(testValue)));
        assertThat(result.ss(), containsInAnyOrder(testValue));
    }

    @Test
    void testConvertNumberSet() {
        final String testValue = "123";
        final AttributeValue result = CONVERTER.convertToAttributeValue(new DynamodbNumberSet(List.of(testValue)));
        assertThat(result.ns(), containsInAnyOrder(testValue));
    }

    @Test
    void testConvertBinarySet() {
        final SdkBytes testValue = SdkBytes.fromUtf8String("test");
        final AttributeValue result = CONVERTER.convertToAttributeValue(new DynamodbBinarySet(List.of(testValue)));
        assertThat(result.bs(), containsInAnyOrder(testValue));
    }

    @Test
    void testConvertList() {
        final AttributeValue testValue = AttributeValueQuickCreator.forString("test");
        final AttributeValue result = CONVERTER.convertToAttributeValue(new DynamodbList(List.of(testValue)));
        assertThat(result.l(), containsInAnyOrder(testValue));
    }

    @Test
    void testConvertMap() {
        final Map<String, AttributeValue> testValue = Map.of("key", AttributeValueQuickCreator.forString("test"));
        final AttributeValue result = CONVERTER.convertToAttributeValue(new DynamodbMap(testValue));
        assertThat(result.m(), equalTo(testValue));
    }

    @Test
    void testConvertNull() {
        final AttributeValue result = CONVERTER.convertToAttributeValue(new DynamodbNull());
        assertThat(result.nul(), equalTo(true));
    }
}