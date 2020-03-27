package com.exasol.dynamodb.resultwalker;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * This {@link DynamodbResultWalker} does no step in the path. It is used for
 * representing the root node.
 */
public class IdentityDynamodbResultWalker extends DynamodbResultWalker {
	private static final long serialVersionUID = -4471805229610023302L;

	/**
	 * Constructor as last part of the {@link DynamodbResultWalker} chain.
	 */
	public IdentityDynamodbResultWalker() {
		super();
	}

	/**
	 * Constructor as non last part of the {@link DynamodbResultWalker} chain.
	 */
	public IdentityDynamodbResultWalker(final DynamodbResultWalker next) {
		super(next);
	}

	@Override
	String stepDescription() {
		return "";
	}

	@Override
	AttributeValue applyThis(final AttributeValue attributeValue, final String path) {
		return attributeValue;
	}

	/**
	 * Builder for {@link IdentityDynamodbResultWalker}
	 */
	public static class Builder extends DynamodbResultWalkerBuilder {
		@Override
		public DynamodbResultWalker buildChain(final DynamodbResultWalker next) {
			return new IdentityDynamodbResultWalker(next);
		}
	}
}
