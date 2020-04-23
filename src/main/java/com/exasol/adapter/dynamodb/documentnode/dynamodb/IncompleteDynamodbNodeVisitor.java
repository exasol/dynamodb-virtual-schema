package com.exasol.adapter.dynamodb.documentnode.dynamodb;

/**
 * This facade for the {@link DynamodbNodeVisitor} interface implements all method with a default implementation that
 * calls the {@link #defaultVisit(String)} with the type as string. Is is used to keep code readable if only a few
 * methods are implemented.
 */
public interface IncompleteDynamodbNodeVisitor extends DynamodbNodeVisitor {

    @Override
    public default void visit(final DynamodbString string) {
        defaultVisit("String");
    }

    @Override
    public default void visit(final DynamodbNumber number) {
        defaultVisit("Number");
    }

    @Override
    public default void visit(final DynamodbBinary binary) {
        defaultVisit("Binary");
    }

    @Override
    public default void visit(final DynamodbBoolean bool) {
        defaultVisit("Boolean");
    }

    @Override
    public default void visit(final DynamodbStringSet stringSet) {
        defaultVisit("StringSet");
    }

    @Override
    public default void visit(final DynamodbBinarySet binarySet) {
        defaultVisit("BinarySet");
    }

    @Override
    public default void visit(final DynamodbNumberSet numberSet) {
        defaultVisit("NumberSet");
    }

    @Override
    public default void visit(final DynamodbList list) {
        defaultVisit("List");
    }

    @Override
    public default void visit(final DynamodbMap map) {
        defaultVisit("Object");
    }

    @Override
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
