package com.exasol.adapter.sql;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.document.queryplanning.RemoteTableQueryFactory;

/**
 * Facade for the {@link SqlNodeVisitor} interface implementing all methods with unsupported exception. This class is
 * used for keeping {@link RemoteTableQueryFactory} short and readable.
 */
public abstract class VoidSqlNodeVisitor implements SqlNodeVisitor<Void> {

    @Override
    public Void visit(final SqlStatementSelect select) throws AdapterException {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlSelectList selectList) {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlGroupBy groupBy) {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlColumn sqlColumn) {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlFunctionAggregate sqlFunctionAggregate) {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlFunctionAggregateGroupConcat sqlFunctionAggregateGroupConcat) {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlFunctionScalar sqlFunctionScalar) {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlFunctionScalarCase sqlFunctionScalarCase) {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlFunctionScalarCast sqlFunctionScalarCast) {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlFunctionScalarExtract sqlFunctionScalarExtract) {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlLimit sqlLimit) {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlLiteralBool sqlLiteralBool) {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlLiteralDate sqlLiteralDate) {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlLiteralDouble sqlLiteralDouble) {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlLiteralExactnumeric sqlLiteralExactnumeric) {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlLiteralNull sqlLiteralNull) {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlLiteralString sqlLiteralString) {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlLiteralTimestamp sqlLiteralTimestamp) {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlLiteralTimestampUtc sqlLiteralTimestampUtc) {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlLiteralInterval sqlLiteralInterval) {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlOrderBy sqlOrderBy) {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlPredicateAnd sqlPredicateAnd) {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlPredicateBetween sqlPredicateBetween) {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlPredicateEqual sqlPredicateEqual) {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlPredicateInConstList sqlPredicateInConstList) {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlPredicateLess sqlPredicateLess) {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlPredicateLessEqual sqlPredicateLessEqual) {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlPredicateLike sqlPredicateLike) {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlPredicateLikeRegexp sqlPredicateLikeRegexp) {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlPredicateNot sqlPredicateNot) {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlPredicateNotEqual sqlPredicateNotEqual) {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlPredicateOr sqlPredicateOr) {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlPredicateIsNotNull sqlPredicateOr) {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlPredicateIsNull sqlPredicateOr) {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlTable sqlTable) {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlJoin sqlJoin) {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlPredicateIsJson sqlPredicateIsJson) throws AdapterException {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlPredicateIsNotJson sqlPredicateIsNotJson) throws AdapterException {
        visitUnimplemented();
        return null;
    }

    @Override
    public Void visit(final SqlFunctionScalarJsonValue sqlFunctionScalarJsonValue) throws AdapterException {
        visitUnimplemented();
        return null;
    }

    /**
     * This method is called when the specific visit method for a type was not implemented.
     */
    public void visitUnimplemented() {
        throw new UnsupportedOperationException("A handling for this SQL statement part was not implemented yet.");
    }
}
