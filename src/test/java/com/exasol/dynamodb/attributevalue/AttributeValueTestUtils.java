package com.exasol.dynamodb.attributevalue;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * Util class that provides factory methods for quickly creating
 * {@link AttributeValue}s
 */
public class AttributeValueTestUtils {

	/**
	 * Creates a {@link AttributeValue} for a given string
	 * 
	 * @param string
	 *            String to be stored
	 * @return {@link AttributeValue}
	 */
	public static AttributeValue forString(final String string) {
		final AttributeValue attributeValue = new AttributeValue();
		attributeValue.setS(string);
		return attributeValue;
	}

	/**
	 * Creates a {@link AttributeValue} for a given number
	 * 
	 * @param string
	 *            String containing the number
	 * @return {@link AttributeValue}
	 */
	public static AttributeValue forNumber(final String string) {
		final AttributeValue attributeValue = new AttributeValue();
		attributeValue.setN(string);
		return attributeValue;
	}
}
