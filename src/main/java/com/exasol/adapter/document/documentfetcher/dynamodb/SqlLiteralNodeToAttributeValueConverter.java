package com.exasol.adapter.document.documentfetcher.dynamodb;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.sql.*;
import com.exasol.errorreporting.ExaError;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * This class converts a {@link SqlNode} literal to an DynamoDB {@link AttributeValue}.
 */
public class SqlLiteralNodeToAttributeValueConverter {

    /**
     * Convert a {@link SqlNode} literal to an DynamoDB {@link AttributeValue}.
     * 
     * @param sqlNode {@link SqlNode} literal
     * @return DynamoDB {@link AttributeValue}
     */
    public AttributeValue convert(final SqlNode sqlNode) {
        final ConvertVisitor visitor = new ConvertVisitor();
        try {
            sqlNode.accept(visitor);
        } catch (final AdapterException exception) {
            throw new IllegalStateException(ExaError.messageBuilder("F-VSDY-31")
                    .message("An unexpected error occurred during conversion from SqlNode to DynamoDB AttributeValue.")
                    .toString(), exception);
        }
        return visitor.getResult();
    }

    private static class ConvertVisitor extends VoidSqlNodeVisitor {
        private AttributeValue result;

        @Override
        public Void visit(final SqlLiteralBool sqlLiteralBool) {
            this.result = AttributeValue.builder().bool(sqlLiteralBool.getValue()).build();
            return null;
        }

        @Override
        public Void visit(final SqlLiteralDouble sqlLiteralDouble) {
            this.result = AttributeValue.builder().n(String.valueOf(sqlLiteralDouble.getValue())).build();
            return null;
        }

        @Override
        public Void visit(final SqlLiteralExactnumeric sqlLiteralExactnumeric) {
            this.result = AttributeValue.builder().n(sqlLiteralExactnumeric.getValue().toString()).build();
            return null;
        }

        @Override
        public Void visit(final SqlLiteralNull sqlLiteralNull) {
            this.result = AttributeValue.builder().nul(true).build();
            return null;
        }

        @Override
        public Void visit(final SqlLiteralString sqlLiteralString) {
            this.result = AttributeValue.builder().s(sqlLiteralString.getValue()).build();
            return null;
        }

        @Override
        public void visitUnimplemented() {
            throw new IllegalStateException(ExaError.messageBuilder("E-VSDY-32")
                    .message("Could not convert SqlNode to DynamoDB AttributeValue.").ticketMitigation().toString());
        }

        public AttributeValue getResult() {
            return this.result;
        }
    }
}
