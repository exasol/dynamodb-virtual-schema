package com.exasol.dynamodb;

import java.util.List;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.dynamodb.attributevalue.AttributeValueVisitor;
import com.exasol.dynamodb.attributevalue.AttributeValueWrapper;

/**
 * Converts a DynamoDB {@link com.amazonaws.services.dynamodbv2.model.AttributeValue} to JSON
 */
public class AttributeValueToJsonConverter {

    private AttributeValueToJsonConverter() {

    }

    /**
     * Converts an DynamoDB {@link AttributeValue} to json
     *
     * @param attributeValue DynamoDB value
     * @return JSON value
     */
    public static JsonValue convert(final AttributeValue attributeValue) {
        final AttributeValueWrapper wrapper = new AttributeValueWrapper(attributeValue);
        final ToJsonVisitor toJsonVisitor = new ToJsonVisitor();
        wrapper.accept(toJsonVisitor);
        return toJsonVisitor.getJsonValue();
    }

    private static class ToJsonVisitor implements AttributeValueVisitor {
        JsonValue jsonValue;

        @Override
        public void visitNull() {
            this.jsonValue = JsonValue.NULL;
        }

        @Override
        public void visitMap(final Map<String, AttributeValue> map) {
            final JsonObjectBuilder objectBuilder = Json.createObjectBuilder();
            for (final Map.Entry<String, AttributeValue> mapEntry : map.entrySet()) {
                objectBuilder.add(mapEntry.getKey(), AttributeValueToJsonConverter.convert(mapEntry.getValue()));
            }
            this.jsonValue = objectBuilder.build();
        }

        @Override
        public void visitString(final String value) {
            this.jsonValue = Json.createValue(value);
        }

        private JsonValue convertNumber(final String value) {
            if (value.contains(".")) {
                final double number = Double.parseDouble(value);
                return Json.createValue(number);
            } else {
                final long number = Long.parseLong(value);
                return Json.createValue(number);
            }
        }

        @Override
        public void visitNumber(final String value) {
            this.jsonValue = convertNumber(value);
        }

        @Override
        public void visitList(final List<AttributeValue> list) {
            final JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();
            for (final AttributeValue attributeValue : list) {
                arrayBuilder.add(AttributeValueToJsonConverter.convert(attributeValue));
            }
            this.jsonValue = arrayBuilder.build();
        }

        @Override
        public void visitBoolean(final boolean value) {
            if (value) {
                this.jsonValue = JsonValue.TRUE;
            } else {
                this.jsonValue = JsonValue.FALSE;
            }
        }

        @Override
        public void visitNumberSet(final List<String> numbers) {
            final JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();
            for (final String number : numbers) {
                arrayBuilder.add(this.convertNumber(number));
            }
            this.jsonValue = arrayBuilder.build();
        }

        @Override
        public void visitStringSet(final List<String> strings) {
            final JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();
            for (final String string : strings) {
                arrayBuilder.add(string);
            }
            this.jsonValue = arrayBuilder.build();
        }

        /**
         * Getter for the conversion result
         *
         * @return JSON value
         */
        public JsonValue getJsonValue() {
            return this.jsonValue;
        }
    }
}
