package com.exasol.adapter.dynamodb.literalconverter;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.sql.*;

/**
 * This class converts an Exasol Literal to an DynamoDB {@link AttributeValue}.
 */
public class SqlToDynamodbLiteralConverter {

    public AttributeValue convert(final SqlNode exasolLiteralNode) throws NotALiteralException {
        try {
            final Converter converter = new Converter();
            exasolLiteralNode.accept(converter);
            return converter.getAttributeValue();
        } catch (final AdapterException exception) {
            // This should never happen, as we do not throw adapter exceptions in the visitor.
            throw new IllegalStateException("An unexpected adapter exception occurred", exception);
        } catch (final NotALiteralExceptionWrapper wrapper) {
            throw wrapper.getException();
        }
    }

    private static class Converter extends VoidSqlNodeVisitor {
        AttributeValue attributeValue = new AttributeValue();

        @Override
        public Void visit(final SqlLiteralString sqlLiteralString) {
            this.attributeValue.setS(sqlLiteralString.getValue());
            return null;
        }

        @Override
        public Void visit(final SqlLiteralBool sqlLiteralBool) {
            this.attributeValue.setBOOL(sqlLiteralBool.getValue());
            return null;
        }

        @Override
        public Void visit(final SqlLiteralDate sqlLiteralDate) {
            throw buildUnsupportedTypeException("date");
        }

        @Override
        public Void visit(final SqlLiteralDouble sqlLiteralDouble) {
            this.attributeValue.setN(String.valueOf(sqlLiteralDouble.getValue()));
            return null;
        }

        @Override
        public Void visit(final SqlLiteralExactnumeric sqlLiteralExactnumeric) {
            this.attributeValue.setN(String.valueOf(sqlLiteralExactnumeric.getValue()));
            return null;
        }

        @Override
        public Void visit(final SqlLiteralNull sqlLiteralNull) {
            this.attributeValue.setNULL(true);
            return null;
        }

        @Override
        public Void visit(final SqlLiteralTimestamp sqlLiteralTimestamp) {
            throw buildUnsupportedTypeException("timestamp");
        }

        @Override
        public Void visit(final SqlLiteralTimestampUtc sqlLiteralTimestampUtc) {
            throw buildUnsupportedTypeException("timestamp utc");
        }

        @Override
        public Void visit(final SqlLiteralInterval sqlLiteralInterval) {
            throw buildUnsupportedTypeException("interval");
        }

        private AttributeValue getAttributeValue() {
            return this.attributeValue;
        }

        @Override
        public void visitDefault() {
            throw new NotALiteralExceptionWrapper(new NotALiteralException());
        }

        public UnsupportedOperationException buildUnsupportedTypeException(final String type) {
            return new UnsupportedOperationException("DynamoDB has no corresponding literal for Exasol's " + type
                    + " literal. Please remove this literal from the Capabilities.");
        }
    }

    private static class NotALiteralExceptionWrapper extends RuntimeException {
        private final NotALiteralException exception;

        private NotALiteralExceptionWrapper(final NotALiteralException exception) {
            this.exception = exception;
        }

        private NotALiteralException getException() {
            return this.exception;
        }
    }
}
