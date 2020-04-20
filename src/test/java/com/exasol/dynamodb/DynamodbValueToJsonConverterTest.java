package com.exasol.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import javax.json.JsonValue;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.*;
import com.exasol.dynamodb.attributevalue.AttributeValueQuickCreator;

public class DynamodbValueToJsonConverterTest {

    @Test
    void testConvertString() {
        final String testString = "test";
        final JsonValue json = new DynamodbValueToJsonConverter().convert(new DynamodbString(testString));
        assertThat(json.toString(), equalTo("\"" + testString + "\""));
    }

    void testNumber(final double number) {
        final String numberString = String.valueOf(number);
        final JsonValue json = new DynamodbValueToJsonConverter()
                .convert(new DynamodbNumber(numberString));
        assertThat(json.toString(), equalTo(numberString));
    }

    @Test
    void testConvertInteger() {
        testNumber(1000);
    }

    @Test
    void testConvertDouble() {
        testNumber(123.456);
    }

    @Test
    void testConvertMap() {
        final AttributeValue nestedAttributeValue = AttributeValueQuickCreator.forString("some value");
        final JsonValue json = new DynamodbValueToJsonConverter()
                .convert(new DynamodbMap(Map.of("keyString", nestedAttributeValue)));
        assertThat(json.toString(), equalTo("{\"keyString\":\"some value\"}"));
    }

    @Test
    void testConvertList() {
        final DynamodbList dynamodbList = new DynamodbList(
                List.of(AttributeValueQuickCreator.forString("test1"), AttributeValueQuickCreator.forString("test2")));
        final JsonValue json = new DynamodbValueToJsonConverter().convert(dynamodbList);
        assertThat(json.toString(), equalTo("[\"test1\",\"test2\"]"));
    }

    @Test
    void testConvertTrue() {
        final JsonValue json = new DynamodbValueToJsonConverter().convert(new DynamodbBoolean(true));
        assertThat(json.toString(), equalTo("true"));
    }

    @Test
    void testConvertFalse() {
        final JsonValue json = new DynamodbValueToJsonConverter().convert(new DynamodbBoolean(false));
        assertThat(json.toString(), equalTo("false"));
    }

    @Test
    void testConvertNumberSet() {
        final JsonValue json = new DynamodbValueToJsonConverter().convert(new DynamodbNumberSet(List.of("123", "456")));
        assertThat(json.toString(), equalTo("[123,456]"));
    }

    @Test
    void testConvertStringSet() {
        final JsonValue json = new DynamodbValueToJsonConverter()
                .convert(new DynamodbStringSet(List.of("test", "test2")));
        assertThat(json.toString(), equalTo("[\"test\",\"test2\"]"));
    }

    @Test
    void testBinaryException() {
        assertThrows(UnsupportedOperationException.class,
                () -> new DynamodbValueToJsonConverter().convert(new DynamodbBinary(ByteBuffer.wrap("".getBytes()))));
    }

    @Test
    void testBinarySetException() {
        final DynamodbBinarySet binarySet = new DynamodbBinarySet(List.of(ByteBuffer.wrap("".getBytes())));
        assertThrows(UnsupportedOperationException.class, () -> new DynamodbValueToJsonConverter().convert(binarySet));
    }

    @Test
    void testConvertNull() {
        final JsonValue json = new DynamodbValueToJsonConverter().convert(new DynamodbNull());
        assertThat(json.toString(), equalTo("null"));
    }
}
