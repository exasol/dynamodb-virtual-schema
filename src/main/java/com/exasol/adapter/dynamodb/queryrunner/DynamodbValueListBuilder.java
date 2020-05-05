package com.exasol.adapter.dynamodb.queryrunner;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * This class builds a map that maps placeholders that are used in DynamoDB expressions to an {@link AttributeValue}.
 * The map can be passed to the DynamoDB API.
 */
public class DynamodbValueListBuilder implements DynamodbValueListBuilderAddValueInterface {
    private final Map<String, AttributeValue> valueMap = new HashMap<>();
    private int counter = 0;

    @Override
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
