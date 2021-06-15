package com.exasol.adapter.document.literalconverter.dynamodb;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.document.documentnode.DocumentValue;
import com.exasol.adapter.document.documentnode.dynamodb.*;
import com.exasol.adapter.document.literalconverter.NotLiteralException;
import com.exasol.adapter.document.literalconverter.SqlLiteralToDocumentValueConverter;
import com.exasol.adapter.sql.*;
import com.exasol.errorreporting.ExaError;

public class SqlLiteralToDynamodbValueConverter implements SqlLiteralToDocumentValueConverter<DynamodbNodeVisitor> {
    @Override
    public DocumentValue<DynamodbNodeVisitor> convert(final SqlNode exasolLiteralNode) throws NotLiteralException {
        try {
            final Converter converter = new Converter();
            exasolLiteralNode.accept(converter);
            return converter.getDynamodbValue();
        } catch (final AdapterException exception) {
            // This should never happen, as we do not throw adapter exceptions in the visitor.
            throw new IllegalStateException(ExaError.messageBuilder("F-VS-DY-28")
                    .message("An unexpected adapter exception occurred.").ticketMitigation().toString(), exception);
        } catch (final NotLiteralExceptionWrapper wrapper) {
            throw wrapper.getException();
        }
    }

    private static class Converter extends VoidSqlNodeVisitor {
        DocumentValue<DynamodbNodeVisitor> dynamodbValue;

        @Override
        public Void visit(final SqlLiteralString sqlLiteralString) {
            this.dynamodbValue = new DynamodbString(sqlLiteralString.getValue());
            return null;
        }

        @Override
        public Void visit(final SqlLiteralBool sqlLiteralBool) {
            this.dynamodbValue = new DynamodbBoolean(sqlLiteralBool.getValue());
            return null;
        }

        @Override
        public Void visit(final SqlLiteralDate sqlLiteralDate) {
            throw buildUnsupportedTypeException("date");
        }

        @Override
        public Void visit(final SqlLiteralDouble sqlLiteralDouble) {
            this.dynamodbValue = new DynamodbNumber(String.valueOf(sqlLiteralDouble.getValue()));
            return null;
        }

        @Override
        public Void visit(final SqlLiteralExactnumeric sqlLiteralExactnumeric) {
            this.dynamodbValue = new DynamodbNumber(String.valueOf(sqlLiteralExactnumeric.getValue()));
            return null;
        }

        @Override
        public Void visit(final SqlLiteralNull sqlLiteralNull) {
            this.dynamodbValue = new DynamodbNull();
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

        private DocumentValue<DynamodbNodeVisitor> getDynamodbValue() {
            return this.dynamodbValue;
        }

        @Override
        public void visitUnimplemented() {
            throw new NotLiteralExceptionWrapper(new NotLiteralException());
        }

        public UnsupportedOperationException buildUnsupportedTypeException(final String type) {
            return new UnsupportedOperationException(ExaError.messageBuilder("E-VS-DY-29").message(
                    "DynamoDB has no corresponding literal for Exasol's {{exasol literal}} literal. Please remove this literal from the capabilities.",
                    type).toString());
        }
    }

    private static class NotLiteralExceptionWrapper extends RuntimeException {
        private static final long serialVersionUID = -2558397108956339878L;
        private final NotLiteralException exception;

        private NotLiteralExceptionWrapper(final NotLiteralException exception) {
            this.exception = exception;
        }

        private NotLiteralException getException() {
            return this.exception;
        }
    }
}
