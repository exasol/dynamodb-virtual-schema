package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import java.util.HashMap;
import java.util.Map;

/**
 * This class is an abstract basis for classes that build a map that maps placeholders to a value.
 *
 * @param <T> Type of the value to be mapped
 */
public abstract class AbstractDynamodbPlaceholderMapBuilder<T> {
    private final Map<String, T> placeholderMap = new HashMap<>();
    private final Map<T, String> invertedMap = new HashMap<>();
    private int uniquePlaceholderIdentifier = 0;

    /**
     * Get the start character for placeholders. For example {@code #}.
     * 
     * @return start character for the placeholder
     */
    protected abstract String getPlaceholderCharacter();

    /**
     * Add a value to the placeholder map and gives a placeholder for it. This method generates unique placeholders by
     * like {@code #1} by a application specific placeholder character and a number that is increased for each generated
     * placeholder.
     * 
     * @param valueToAdd value to be replaced by a placeholder
     * @return placeholder for the value
     */
    public final String addValue(final T valueToAdd) {
        if (this.invertedMap.containsKey(valueToAdd)) {
            return this.invertedMap.get(valueToAdd);
        }
        final String key = getPlaceholderCharacter() + this.uniquePlaceholderIdentifier++;
        this.placeholderMap.put(key, valueToAdd);
        this.invertedMap.put(valueToAdd, key);
        return key;
    }

    /**
     * Get the built placeholder map.
     * 
     * @return value map
     */
    public final Map<String, T> getPlaceholderMap() {
        return this.placeholderMap;
    }
}
