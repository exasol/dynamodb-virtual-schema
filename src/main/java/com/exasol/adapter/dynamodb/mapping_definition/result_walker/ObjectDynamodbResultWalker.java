package com.exasol.adapter.dynamodb.mapping_definition.result_walker;

import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * This {@link DynamodbResultWalker} does walks on object property. For example
 * applied on the path {@code author.name} the walker returns an AttributeValue
 * of type Map with {@code name} as key.
 */
public class ObjectDynamodbResultWalker extends DynamodbResultWalker {
	private static final long serialVersionUID = -4999583991152944380L;
	private final LookupFailBehaviour lookupFailBehaviour;
	private final String lookupKey;

	/**
	 * Constructor as last part of the {@link DynamodbResultWalker} chain.
	 */
	public ObjectDynamodbResultWalker(final LookupFailBehaviour lookupFailBehaviour, final String lookupKey) {
		this.lookupFailBehaviour = lookupFailBehaviour;
		this.lookupKey = lookupKey;
	}

	/**
	 * Constructor as non last part of the {@link DynamodbResultWalker} chain.
	 */
	public ObjectDynamodbResultWalker(final LookupFailBehaviour lookupFailBehaviour, final String lookupKey,
			final DynamodbResultWalker next) {
		super(next);
		this.lookupFailBehaviour = lookupFailBehaviour;
		this.lookupKey = lookupKey;
	}

	@Override
	AttributeValue applyThis(final AttributeValue attributeValue) throws DynamodbResultWalkerException {
		if (attributeValue.getM() == null) {
			if (this.lookupFailBehaviour == LookupFailBehaviour.EXCEPTION) {
				throw new DynamodbResultWalkerException("Not an object");
			} else {
				return null;
			}
		}
		final Map<String, AttributeValue> map = attributeValue.getM();
		final AttributeValue nextAttributeValue = map.get(this.lookupKey);
		if (nextAttributeValue == null) {
			if (this.lookupFailBehaviour == LookupFailBehaviour.EXCEPTION) {
				throw new DynamodbResultWalkerException("lookup failed");
			} else {
				return null;
			}
		}
		return nextAttributeValue;
	}
}
