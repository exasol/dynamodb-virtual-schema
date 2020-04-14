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
import com.exasol.dynamodb.attributevalue.AttributeValueTestUtils;

/**
 * Test for {@link AttributeValueToJsonConverter}
 */
public class AttributeValueToJsonConverterTest {

    @Test
    void testConvertString() {
        final String testString = "test";
        final AttributeValue attributeValue = new AttributeValue();
        attributeValue.setS(testString);
        final JsonValue json = new AttributeValueToJsonConverter().convert(attributeValue);
        assertThat(json.toString(), equalTo("\"" + testString + "\""));
    }

    void testNumber(final double number) {
        final String numberString = String.valueOf(number);
        final AttributeValue attributeValue = new AttributeValue();
        attributeValue.setN(numberString);
        final JsonValue json = new AttributeValueToJsonConverter().convert(attributeValue);
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
        final AttributeValue nestedAttributeValue = AttributeValueTestUtils.forString("some value");
        final AttributeValue testMap = new AttributeValue();
        testMap.setM(Map.of("keyString", nestedAttributeValue));
        final JsonValue json = new AttributeValueToJsonConverter().convert(testMap);
        assertThat(json.toString(), equalTo("{\"keyString\":\"some value\"}"));
    }

    @Test
    void testConvertList() {
        final AttributeValue attributeValue = new AttributeValue();
        attributeValue
                .setL(List.of(AttributeValueTestUtils.forString("test1"), AttributeValueTestUtils.forString("test2")));
        final JsonValue json = new AttributeValueToJsonConverter().convert(attributeValue);
        assertThat(json.toString(), equalTo("[\"test1\",\"test2\"]"));
    }

    @Test
    void testConvertTrue() {
        final AttributeValue attributeValue = new AttributeValue();
        attributeValue.setBOOL(true);
        final JsonValue json = new AttributeValueToJsonConverter().convert(attributeValue);
        assertThat(json.toString(), equalTo("true"));
    }

    @Test
    void testConvertFalse() {
        final AttributeValue attributeValue = new AttributeValue();
        attributeValue.setBOOL(false);
        final JsonValue json = new AttributeValueToJsonConverter().convert(attributeValue);
        assertThat(json.toString(), equalTo("false"));
    }

    @Test
    void testConvertNumberSet() {
        final AttributeValue attributeValue = new AttributeValue();
        attributeValue.setNS(List.of("123", "456"));
        final JsonValue json = new AttributeValueToJsonConverter().convert(attributeValue);
        assertThat(json.toString(), equalTo("[123,456]"));
    }

    @Test
    void testConvertStringSet() {
        final AttributeValue attributeValue = new AttributeValue();
        attributeValue.setSS(List.of("test", "test2"));
        final JsonValue json = new AttributeValueToJsonConverter().convert(attributeValue);
        assertThat(json.toString(), equalTo("[\"test\",\"test2\"]"));
    }

    @Test
    void testBinaryException() {
        final AttributeValue attributeValue = new AttributeValue();
        attributeValue.setB(ByteBuffer.wrap("".getBytes()));
        assertThrows(UnsupportedOperationException.class,
                () -> new AttributeValueToJsonConverter().convert(attributeValue));
    }

    @Test
    void testBinarySetException() {
        final AttributeValue attributeValue = new AttributeValue();
        attributeValue.setBS(List.of(ByteBuffer.wrap("".getBytes())));
        assertThrows(UnsupportedOperationException.class,
                () -> new AttributeValueToJsonConverter().convert(attributeValue));
    }

    @Test
    void testConvertNull() {
        final AttributeValue attributeValue = new AttributeValue();
        attributeValue.setNULL(true);
        final JsonValue json = new AttributeValueToJsonConverter().convert(attributeValue);
        assertThat(json.toString(), equalTo("null"));
    }
}
