package com.exasol.dynamodb.resultwalker;

public abstract class DynamodbResultWalkerBuilder {
	abstract DynamodbResultWalker buildChain(DynamodbResultWalker next);
	public DynamodbResultWalker build() {
		return this.buildChain(null);
	}
}
