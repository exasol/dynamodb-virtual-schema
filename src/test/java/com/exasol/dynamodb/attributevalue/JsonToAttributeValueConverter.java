package com.exasol.dynamodb.attributevalue;

import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.exasol.errorreporting.ExaError;

import jakarta.json.*;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class JsonToAttributeValueConverter {
    private static final JsonToAttributeValueConverter INSTANCE = new JsonToAttributeValueConverter();

    /**
     * Private constructor to hide the public default.
     */
    private JsonToAttributeValueConverter() {
        /* empty on purpose */
    }

    public static JsonToAttributeValueConverter getInstance() {
        return INSTANCE;
    }

    public Map<String, AttributeValue> convert(final String json) {
        final JsonObject jsonObject = Json.createReader(new StringReader(json)).readObject();
        return convertJsonObject(jsonObject);
    }

    private AttributeValue convertJsonValue(final JsonValue value) {
        switch (value.getValueType()) {
        case NUMBER:
            final JsonNumber jsonNumber = (JsonNumber) value;
            return AttributeValue.builder().n(jsonNumber.toString()).build();
        case STRING:
            final JsonString jsonString = (JsonString) value;
            return AttributeValue.builder().s(jsonString.getString()).build();
        case TRUE:
            return AttributeValue.builder().bool(true).build();
        case FALSE:
            return AttributeValue.builder().bool(false).build();
        case NULL:
            return AttributeValue.builder().nul(true).build();
        case ARRAY:
            final JsonArray jsonArray = (JsonArray) value;
            final List<AttributeValue> convertedListItems = jsonArray.stream().map(this::convertJsonValue)
                    .collect(Collectors.toList());
            return AttributeValue.builder().l(convertedListItems).build();
        case OBJECT:
            final JsonObject jsonObject = (JsonObject) value;
            return AttributeValue.builder().m(convertJsonObject(jsonObject)).build();
        default:
            throw new UnsupportedOperationException(ExaError.messageBuilder("F-VS-DY-18").message(
                    "Failed to convert JSON value of type {{type}} to DynamoDB attribute value. This conversion is not yet implemented.",
                    value.getValueType().toString()).ticketMitigation().toString());
        }
    }

    private Map<String, AttributeValue> convertJsonObject(final JsonObject jsonObject) {
        return jsonObject.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, each -> convertJsonValue(each.getValue())));
    }
}
