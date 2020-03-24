package com.exasol.adapter.dynamodb.mapping_definition.result_walker;

public abstract class DynamodbResultWalkerBuilder {
	abstract DynamodbResultWalker buildChain(DynamodbResultWalker next);
	public DynamodbResultWalker build() {
		return this.buildChain(null);
	}
}
