package com.exasol.adapter.dynamodb.literalconverter.dynamodb;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dynamodb.documentnode.DocumentValue;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.*;
import com.exasol.adapter.dynamodb.literalconverter.NotALiteralException;
import com.exasol.adapter.dynamodb.literalconverter.SqlLiteralToDocumentValueConverter;
import com.exasol.adapter.sql.*;

public class SqlLiteralToDynamodbValueConverter implements SqlLiteralToDocumentValueConverter<DynamodbNodeVisitor> {
    @Override
    public DocumentValue<DynamodbNodeVisitor> convert(final SqlNode exasolLiteralNode) throws NotALiteralException {
        try {
            final Converter converter = new Converter();
            exasolLiteralNode.accept(converter);
            return converter.getDynmaodbValue();
        } catch (final AdapterException exception) {
            // This should never happen, as we do not throw adapter exceptions in the visitor.
            throw new IllegalStateException("An unexpected adapter exception occurred", exception);
        } catch (final NotALiteralExceptionWrapper wrapper) {
            throw wrapper.getException();
        }
    }

    private static class Converter extends VoidSqlNodeVisitor {
        DocumentValue<DynamodbNodeVisitor> dynmaodbValue;

        @Override
        public Void visit(final SqlLiteralString sqlLiteralString) {
            this.dynmaodbValue = new DynamodbString(sqlLiteralString.getValue());
            return null;
        }

        @Override
        public Void visit(final SqlLiteralBool sqlLiteralBool) {
            this.dynmaodbValue = new DynamodbBoolean(sqlLiteralBool.getValue());
            return null;
        }

        @Override
        public Void visit(final SqlLiteralDate sqlLiteralDate) {
            throw buildUnsupportedTypeException("date");
        }

        @Override
        public Void visit(final SqlLiteralDouble sqlLiteralDouble) {
            this.dynmaodbValue = new DynamodbNumber(String.valueOf(sqlLiteralDouble.getValue()));
            return null;
        }

        @Override
        public Void visit(final SqlLiteralExactnumeric sqlLiteralExactnumeric) {
            this.dynmaodbValue = new DynamodbNumber(String.valueOf(sqlLiteralExactnumeric.getValue()));
            return null;
        }

        @Override
        public Void visit(final SqlLiteralNull sqlLiteralNull) {
            this.dynmaodbValue = new DynamodbNull();
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

        private DocumentValue<DynamodbNodeVisitor> getDynmaodbValue() {
            return this.dynmaodbValue;
        }

        @Override
        public void visitDefault() {
            throw new NotALiteralExceptionWrapper(new NotALiteralException());
        }

        public UnsupportedOperationException buildUnsupportedTypeException(final String type) {
            return new UnsupportedOperationException("DynamoDB has no corresponding literal for Exasol's " + type
                    + " literal. Please remove this literal from the capabilities.");
        }
    }

    private static class NotALiteralExceptionWrapper extends RuntimeException {
        private static final long serialVersionUID = -2558397108956339878L;
        private final NotALiteralException exception;

        private NotALiteralExceptionWrapper(final NotALiteralException exception) {
            this.exception = exception;
        }

        private NotALiteralException getException() {
            return this.exception;
        }
    }
}
