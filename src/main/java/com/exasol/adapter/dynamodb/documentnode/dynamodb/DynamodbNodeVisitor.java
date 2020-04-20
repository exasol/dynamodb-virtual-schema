package com.exasol.adapter.dynamodb.documentnode.dynamodb;

/**
 * Visitor for Dynamodb document nodes.
 */
public interface DynamodbNodeVisitor {
    public default void visit(final DynamodbString string) {
        defaultVisit("String");
    }

    public default void visit(final DynamodbNumber number) {
        defaultVisit("Number");
    }

    public default void visit(final DynamodbBinary binary) {
        defaultVisit("Binary");
    }

    public default void visit(final DynamodbBoolean bool) {
        defaultVisit("Boolean");
    }

    public default void visit(final DynamodbStringSet stringSet) {
        defaultVisit("StringSet");
    }

    public default void visit(final DynamodbBinarySet binarySet) {
        defaultVisit("BinarySet");
    }

    public default void visit(final DynamodbNumberSet numberSet) {
        defaultVisit("NumberSet");
    }

    public default void visit(final DynamodbList list) {
        defaultVisit("List");
    }

    public default void visit(final DynamodbMap map) {
        defaultVisit("Object");
    }

    public default void visit(final DynamodbNull nullValue) {
        defaultVisit("Null");
    }

    /**
     * Called when the specific visit method was not implemented. This method can for example be used for throwing an
     * {@link UnsupportedOperationException}.
     *
     * @param typeName name of the DynamoDB type to visit.
     */
    public void defaultVisit(final String typeName);
}
