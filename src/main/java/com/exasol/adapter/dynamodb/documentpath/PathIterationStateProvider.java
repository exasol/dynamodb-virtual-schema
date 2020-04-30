package com.exasol.adapter.dynamodb.documentpath;

public interface PathIterationStateProvider {
    public int getIndexFor(DocumentPathExpression pathToArrayAll);
}
