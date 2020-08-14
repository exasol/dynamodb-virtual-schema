package com.exasol.dynamodb.attributevalue;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class JsonToAttributeValueConverterTest {

    @Test
    void testConvertNumber() {
        final String json = "{\"test\": 123}";
        final Map<String, AttributeValue> result = JsonToAttributeValueConverter.getInstance().convert(json);
        assertThat(result.get("test").n(), equalTo("123"));
    }

    @Test
    void testConvertString() {
        final String json = "{\"test\": \"testString\"}";
        final Map<String, AttributeValue> result = JsonToAttributeValueConverter.getInstance().convert(json);
        assertThat(result.get("test").s(), equalTo("testString"));
    }

    @Test
    void testConvertNull() {
        final String json = "{\"test\": null}";
        final Map<String, AttributeValue> result = JsonToAttributeValueConverter.getInstance().convert(json);
        assertThat(result.get("test").nul(), equalTo(true));
    }

    @Test
    void testConvertTrue() {
        final String json = "{\"test\": true}";
        final Map<String, AttributeValue> result = JsonToAttributeValueConverter.getInstance().convert(json);
        assertThat(result.get("test").bool(), equalTo(true));
    }

    @Test
    void testConvertFalse() {
        final String json = "{\"test\": false}";
        final Map<String, AttributeValue> result = JsonToAttributeValueConverter.getInstance().convert(json);
        assertThat(result.get("test").bool(), equalTo(false));
    }

    @Test
    void testConvertArray() {
        final String json = "{\"test\": [1,2]}";
        final Map<String, AttributeValue> result = JsonToAttributeValueConverter.getInstance().convert(json);
        assertThat(result.get("test").l().stream().map(AttributeValue::n).collect(Collectors.toList()),
                containsInAnyOrder("1", "2"));
    }

    @Test
    void testConvertObject() {
        final String json = "{\"test\": {\"a\": 1}}";
        final Map<String, AttributeValue> result = JsonToAttributeValueConverter.getInstance().convert(json);
        assertThat(result.get("test").m().get("a").n(), equalTo("1"));
    }
}