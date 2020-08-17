package com.exasol.adapter.document.literalconverter;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.sql.*;
import com.exasol.sql.expression.*;

/**
 * This class converts a {@link SqlNode} literal into a {@link ValueExpression}.
 */
public class SqlLiteralToValueExpressionConverter {
    private static final SqlLiteralToValueExpressionConverter INSTANCE = new SqlLiteralToValueExpressionConverter();

    /**
     * Private constructor to hide the public default.
     */
    private SqlLiteralToValueExpressionConverter() {
        // empty on purpose
    }

    /**
     * Get singleton instance of the {@link SqlLiteralToValueExpressionConverter}.
     *
     * @return instance of the {@link SqlLiteralToValueExpressionConverter}
     */
    public static SqlLiteralToValueExpressionConverter getInstance() {
        return INSTANCE;
    }

    /**
     * Converts a {@link SqlNode} literal into a {@link ValueExpression}.
     *
     * @param sqlNode to convert
     * @return {@link ValueExpression} with the literal value
     */
    public ValueExpression convert(final SqlNode sqlNode) {
        final Visitor visitor = new Visitor();
        try {
            sqlNode.accept(visitor);
            return visitor.getResult();
        } catch (final AdapterException e) {
            throw new IllegalStateException("This should never happen as no AdapterException is thrown in the visitor");
        }
    }

    private static class Visitor extends VoidSqlNodeVisitor {
        private ValueExpression result;

        @Override
        public Void visit(final SqlLiteralBool sqlLiteralBool) {
            this.result = BooleanLiteral.of(sqlLiteralBool.getValue());
            return null;
        }

        @Override
        public Void visit(final SqlLiteralDate sqlLiteralDate) {
            throw new UnsupportedOperationException("There is no ValueExpression for date literals");
        }

        @Override
        public Void visit(final SqlLiteralDouble sqlLiteralDouble) {
            this.result = DoubleLiteral.of(sqlLiteralDouble.getValue());
            return null;
        }

        @Override
        public Void visit(final SqlLiteralExactnumeric sqlLiteralExactnumeric) {
            if (sqlLiteralExactnumeric.getValue().scale() == 0) {
                this.result = LongLiteral.of(sqlLiteralExactnumeric.getValue().longValue());
            } else {
                this.result = DoubleLiteral.of(sqlLiteralExactnumeric.getValue().doubleValue());
            }
            return null;
        }

        @Override
        public Void visit(final SqlLiteralNull sqlLiteralNull) {
            this.result = NullLiteral.nullLiteral();
            return null;
        }

        @Override
        public Void visit(final SqlLiteralString sqlLiteralString) {
            this.result = StringLiteral.of(sqlLiteralString.getValue());
            return null;
        }

        @Override
        public Void visit(final SqlLiteralTimestamp sqlLiteralTimestamp) {
            throw new UnsupportedOperationException("There is no ValueExpression for timestamp literals");
        }

        @Override
        public Void visit(final SqlLiteralTimestampUtc sqlLiteralTimestampUtc) {
            throw new UnsupportedOperationException("There is no ValueExpression for timestamp utc literals");
        }

        @Override
        public Void visit(final SqlLiteralInterval sqlLiteralInterval) {
            throw new UnsupportedOperationException("There is no ValueExpression for interval literals");
        }

        @Override
        public void visitUnimplemented() {
            throw new IllegalArgumentException("The given SqlNode is not a literal");
        }

        public ValueExpression getResult() {
            return this.result;
        }
    }
}
