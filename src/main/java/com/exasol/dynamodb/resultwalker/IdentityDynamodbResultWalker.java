package com.exasol.dynamodb.resultwalker;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * This {@link AbstractDynamodbResultWalker} does no step in the path. It is used for representing the root node.
 */
public class IdentityDynamodbResultWalker extends AbstractDynamodbResultWalker {
    private static final long serialVersionUID = -4471805229610023302L;

    /**
     * Constructor as last part of the {@link AbstractDynamodbResultWalker} chain.
     */
    public IdentityDynamodbResultWalker() {
        super();
    }

    /**
     * Constructor as non last part of the {@link AbstractDynamodbResultWalker} chain.
     */
    public IdentityDynamodbResultWalker(final AbstractDynamodbResultWalker next) {
        super(next);
    }

    @Override
    protected String stepDescription() {
        return "";
    }

    @Override
    protected AttributeValue applyThis(final AttributeValue attributeValue, final String path) {
        return attributeValue;
    }

    /**
     * Builder for {@link IdentityDynamodbResultWalker}
     */
    public static class Builder extends AbstractDynamodbResultWalkerBuilder {
        @Override
        public AbstractDynamodbResultWalker buildChain(final AbstractDynamodbResultWalker next) {
            return new IdentityDynamodbResultWalker(next);
        }
    }
}
