package com.exasol.dynamodb;

import java.util.Map;

import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;
import javax.json.spi.JsonProvider;

import com.exasol.adapter.document.documentnode.DocumentArray;
import com.exasol.adapter.document.documentnode.DocumentNode;
import com.exasol.adapter.document.documentnode.dynamodb.*;

/**
 * This class represents a converter from a DynamoDB values to the JSON format.
 */
public class DynamodbValueToJsonConverter {
    private static final DynamodbValueToJsonConverter INSTANCE = new DynamodbValueToJsonConverter();
    // This is an performance optimization as JsonProvider.provider() is quite slow
    private static final JsonProvider JSON = JsonProvider.provider();

    /**
     * Private constructor to hide the public default.
     */
    private DynamodbValueToJsonConverter() {
        // empty on purpose
    }

    /**
     * Get a singleton instance of {@link DynamodbValueToJsonConverter}.
     *
     * @return instance of {@link DynamodbValueToJsonConverter}
     */
    public static DynamodbValueToJsonConverter getInstance() {
        return INSTANCE;
    }

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
    private static class ToJsonVisitor implements IncompleteDynamodbNodeVisitor {
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
            final JsonObjectBuilder objectBuilder = JSON.createObjectBuilder();
            for (final Map.Entry<String, DocumentNode<DynamodbNodeVisitor>> mapEntry : map.getKeyValueMap()
                    .entrySet()) {
                objectBuilder.add(mapEntry.getKey(), getInstance().convert(mapEntry.getValue()));
            }
            this.jsonValue = objectBuilder.build();
        }

        @Override
        public void visit(final DynamodbString string) {
            this.jsonValue = JSON.createValue(string.getValue());
        }

        @Override
        public void visit(final DynamodbNumber number) {
            this.jsonValue = convertNumber(number.getValue());
        }

        private JsonValue convertNumber(final String value) {
            if (value.contains(".")) {
                final double number = Double.parseDouble(value);
                return JSON.createValue(number);
            } else {
                final long number = Long.parseLong(value);
                return JSON.createValue(number);
            }
        }

        @Override
        public void visit(final DynamodbList list) {
            this.jsonValue = convertList(list);
        }

        private JsonValue convertList(final DocumentArray<DynamodbNodeVisitor> list) {
            final JsonArrayBuilder arrayBuilder = JSON.createArrayBuilder();
            for (final DocumentNode<DynamodbNodeVisitor> attributeValue : list.getValuesList()) {
                arrayBuilder.add(getInstance().convert(attributeValue));
            }
            return arrayBuilder.build();
        }

        @Override
        public void visit(final DynamodbBoolean bool) {
            this.jsonValue = bool.getValue() ? JsonValue.TRUE : JsonValue.FALSE;
        }

        @Override
        public void visit(final DynamodbNumberSet numberSet) {
            this.jsonValue = convertList(numberSet);
        }

        @Override
        public void visit(final DynamodbStringSet stringSet) {
            this.jsonValue = convertList(stringSet);
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