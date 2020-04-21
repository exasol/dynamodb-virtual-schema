package com.exasol.dynamodb;

import java.util.Map;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;

import com.exasol.adapter.dynamodb.documentnode.DocumentArray;
import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.*;

/**
 * This class represents a converter from a DynamoDB {@link com.amazonaws.services.dynamodbv2.model.AttributeValue} to
 * the JSON format.
 */
public class DynamodbValueToJsonConverter {

    /**
     * Converts a DynamoDB document node to JSON.
     *
     * @param value DynamoDB value
     * @return JSON value
     */
    public JsonValue convert(final DocumentNode<DynamodbNodeVisitor> value) {
        final ToJsonVisitor toJsonVisitor = new ToJsonVisitor();
        value.accept(toJsonVisitor);
        return toJsonVisitor.getJsonValue();
    }

    /**
     * This visitor converts the visited value into the JSON format.
     */
    private static class ToJsonVisitor implements DynamodbNodeVisitor {
        JsonValue jsonValue;

        @Override
        public void defaultVisit(final String typeName) {
            throw new UnsupportedOperationException("The DynamoDB type " + typeName
                    + " cant't be converted to JSON string. Try using a different mapping.");
        }

        @Override
        public void visit(final DynamodbNull nullValue) {
            this.jsonValue = JsonValue.NULL;
        }

        @Override
        public void visit(final DynamodbMap map) {
            final JsonObjectBuilder objectBuilder = Json.createObjectBuilder();
            for (final Map.Entry<String, DocumentNode<DynamodbNodeVisitor>> mapEntry : map.getKeyValueMap()
                    .entrySet()) {
                objectBuilder.add(mapEntry.getKey(), new DynamodbValueToJsonConverter().convert(mapEntry.getValue()));
            }
            this.jsonValue = objectBuilder.build();
        }

        @Override
        public void visit(final DynamodbString string) {
            this.jsonValue = Json.createValue(string.getValue());
        }

        @Override
        public void visit(final DynamodbNumber number) {
            this.jsonValue = convertNumber(number.getValue());
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
        public void visit(final DynamodbList list) {
            convertList(list);
        }

        void convertList(final DocumentArray<DynamodbNodeVisitor> list) {
            final JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();
            for (final DocumentNode<DynamodbNodeVisitor> attributeValue : list.getValuesList()) {
                arrayBuilder.add(new DynamodbValueToJsonConverter().convert(attributeValue));
            }
            this.jsonValue = arrayBuilder.build();
        }

        @Override
        public void visit(final DynamodbBoolean bool) {
            this.jsonValue = bool.getValue() ? JsonValue.TRUE : JsonValue.FALSE;
        }

        @Override
        public void visit(final DynamodbNumberSet numberSet) {
            convertList(numberSet);
        }

        @Override
        public void visit(final DynamodbStringSet stringSet) {
            convertList(stringSet);
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
