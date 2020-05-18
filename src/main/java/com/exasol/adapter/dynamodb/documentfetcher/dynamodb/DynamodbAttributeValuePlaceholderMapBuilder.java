package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * This class builds a map that maps placeholders that are used in DynamoDB expressions to an {@link AttributeValue}.
 * The map can be passed to the DynamoDB API.
 */
public class DynamodbAttributeValuePlaceholderMapBuilder {
    private final Map<String, AttributeValue> valueMap = new HashMap<>();
    private int counter = 0;

    /**
     * Adds a value to the placeholder map and gives a placeholder for it.
     * 
     * @param valueToAdd value to be replaced by a placeholder
     * @return placeholder for the value
     */
    public String addValue(final AttributeValue valueToAdd) {
        final String key = ":" + this.counter++;
        this.valueMap.put(key, valueToAdd);
        return key;
    }

    /**
     * Gives the built value map.
     * 
     * @return value map
     */
    public Map<String, AttributeValue> getValueMap() {
        return this.valueMap;
    }
}
