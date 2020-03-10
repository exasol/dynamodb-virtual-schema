package com.exasol.adapter.dynamodb.mapping_definition.result_walker;

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
	AttributeValue applyThis(final AttributeValue attributeValue) {
		return attributeValue;
	}
}
