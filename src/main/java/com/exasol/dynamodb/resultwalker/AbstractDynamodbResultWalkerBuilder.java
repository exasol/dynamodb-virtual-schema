package com.exasol.dynamodb.resultwalker;

/**
 * Abstract builder for {@link AbstractDynamodbResultWalker}. Each subclass of {@link AbstractDynamodbResultWalker}
 * should implement their own builder.
 */
public abstract class AbstractDynamodbResultWalkerBuilder {
    protected abstract AbstractDynamodbResultWalker buildChain(AbstractDynamodbResultWalker next);

    /**
     * Builds the {@link AbstractDynamodbResultWalker}
     * 
     * @return {@link AbstractDynamodbResultWalker}
     */
    public AbstractDynamodbResultWalker build() {
        return this.buildChain(null);
    }
}
