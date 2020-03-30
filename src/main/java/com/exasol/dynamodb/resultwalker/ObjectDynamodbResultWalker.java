package com.exasol.dynamodb.resultwalker;

import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * This {@link AbstractDynamodbResultWalker} does walks on object property. For
 * example applied on the path {@code author.name} the walker returns an
 * AttributeValue of type Map with {@code name} as key.
 */
public class ObjectDynamodbResultWalker extends AbstractDynamodbResultWalker {
	private static final long serialVersionUID = -4999583991152944380L;
	private final String lookupKey;

	/**
	 * Constructor as non last part of the {@link AbstractDynamodbResultWalker}
	 * chain.
	 */
	public ObjectDynamodbResultWalker(final String lookupKey, final AbstractDynamodbResultWalker next) {
		super(next);
		this.lookupKey = lookupKey;
	}

	@Override
	protected AttributeValue applyThis(final AttributeValue attributeValue, final String path)
			throws DynamodbResultWalkerException {
		if (attributeValue.getM() == null) {
			throw new DynamodbResultWalkerException("Not an object", path);
		}
		final Map<String, AttributeValue> map = attributeValue.getM();
		final AttributeValue nextAttributeValue = map.get(this.lookupKey);
		if (nextAttributeValue == null) {
			throw new LookupException("lookup failed", path, this.lookupKey);
		}
		return nextAttributeValue;
	}

	@Override
	protected String stepDescription() {
		return "/" + this.lookupKey;
	}

	/**
	 * Builder for {@link ObjectDynamodbResultWalker}
	 */
	public static class Builder extends AbstractDynamodbResultWalkerBuilder {
		private final AbstractDynamodbResultWalkerBuilder previousBuilder;
		private final String lookupKey;

		/**
		 * Constructor.
		 * 
		 * @param previousBuilder
		 *            previous builder in chain
		 * @param lookupKey
		 *            DynamoDB property name
		 */
		public Builder(final AbstractDynamodbResultWalkerBuilder previousBuilder, final String lookupKey) {
			this.previousBuilder = previousBuilder;
			this.lookupKey = lookupKey;
		}

		@Override
		public AbstractDynamodbResultWalker buildChain(final AbstractDynamodbResultWalker next) {
			return this.previousBuilder.buildChain(new ObjectDynamodbResultWalker(this.lookupKey, next));
		}
	}

	/**
	 * Exception that is thrown if the defined DynamoDB property is not present in a
	 * document.
	 */
	public static class LookupException extends DynamodbResultWalkerException {
		private final String missingLookupKey;
		LookupException(final String message, final String currentPath, final String missingKey) {
			super(message, currentPath);
			this.missingLookupKey = missingKey;
		}

		public String getMissingLookupKey() {
			return this.missingLookupKey;
		}
	}
}
