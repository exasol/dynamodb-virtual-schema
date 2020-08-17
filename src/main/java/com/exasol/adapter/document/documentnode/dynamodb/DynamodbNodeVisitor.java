package com.exasol.adapter.document.documentnode.dynamodb;

/**
 * Visitor for Dynamodb document nodes.
 */
public interface DynamodbNodeVisitor {
    public void visit(final DynamodbString string);

    public void visit(final DynamodbNumber number);

    public void visit(final DynamodbBinary binary);

    public void visit(final DynamodbBoolean bool);

    public void visit(final DynamodbStringSet stringSet);

    public void visit(final DynamodbBinarySet binarySet);

    public void visit(final DynamodbNumberSet numberSet);

    public void visit(final DynamodbList list);

    public void visit(final DynamodbMap map);

    public void visit(final DynamodbNull nullValue);
}
