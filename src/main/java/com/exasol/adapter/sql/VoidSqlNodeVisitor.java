package com.exasol.adapter.sql;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dynamodb.queryresultschema.QueryResultTableSchemaBuilder;

/**
 * Facade for the {@link SqlNodeVisitor} interface implementing all methods with unsupported exception. This class is
 * used for keeping {@link QueryResultTableSchemaBuilder} short and readable.
 */
public abstract class VoidSqlNodeVisitor implements SqlNodeVisitor<Void> {

    @Override
    public Void visit(final SqlStatementSelect select) throws AdapterException {
        visitDefault();
        return null;
    }

    @Override
    public Void visit(final SqlSelectList selectList) {
        visitDefault();
        return null;
    }

    @Override
    public Void visit(final SqlGroupBy groupBy) {
        visitDefault();
        return null;
    }

    @Override
    public Void visit(final SqlColumn sqlColumn) {
        visitDefault();
        return null;
    }

    @Override
    public Void visit(final SqlFunctionAggregate sqlFunctionAggregate) {
        visitDefault();
        return null;
    }

    @Override
    public Void visit(final SqlFunctionAggregateGroupConcat sqlFunctionAggregateGroupConcat) {
        visitDefault();
        return null;
    }

    @Override
    public Void visit(final SqlFunctionScalar sqlFunctionScalar) {
        visitDefault();
        return null;
    }

    @Override
    public Void visit(final SqlFunctionScalarCase sqlFunctionScalarCase) {
        visitDefault();
        return null;
    }

    @Override
    public Void visit(final SqlFunctionScalarCast sqlFunctionScalarCast) {
        visitDefault();
        return null;
    }

    @Override
    public Void visit(final SqlFunctionScalarExtract sqlFunctionScalarExtract) {
        visitDefault();
        return null;
    }

    @Override
    public Void visit(final SqlLimit sqlLimit) {
        visitDefault();
        return null;
    }

    @Override
    public Void visit(final SqlLiteralBool sqlLiteralBool) {
        visitDefault();
        return null;
    }

    @Override
    public Void visit(final SqlLiteralDate sqlLiteralDate) {
        visitDefault();
        return null;
    }

    @Override
    public Void visit(final SqlLiteralDouble sqlLiteralDouble) {
        visitDefault();
        return null;
    }

    @Override
    public Void visit(final SqlLiteralExactnumeric sqlLiteralExactnumeric) {
        visitDefault();
        return null;
    }

    @Override
    public Void visit(final SqlLiteralNull sqlLiteralNull) {
        visitDefault();
        return null;
    }

    @Override
    public Void visit(final SqlLiteralString sqlLiteralString) {
        visitDefault();
        return null;
    }

    @Override
    public Void visit(final SqlLiteralTimestamp sqlLiteralTimestamp) {
        visitDefault();
        return null;
    }

    @Override
    public Void visit(final SqlLiteralTimestampUtc sqlLiteralTimestampUtc) {
        visitDefault();
        return null;
    }

    @Override
    public Void visit(final SqlLiteralInterval sqlLiteralInterval) {
        visitDefault();
        return null;
    }

    @Override
    public Void visit(final SqlOrderBy sqlOrderBy) {
        visitDefault();
        return null;
    }

    @Override
    public Void visit(final SqlPredicateAnd sqlPredicateAnd) throws AdapterException {
        visitDefault();
        return null;
    }

    @Override
    public Void visit(final SqlPredicateBetween sqlPredicateBetween) {
        visitDefault();
        return null;
    }

    @Override
    public Void visit(final SqlPredicateEqual sqlPredicateEqual) {
        visitDefault();
        return null;
    }

    @Override
    public Void visit(final SqlPredicateInConstList sqlPredicateInConstList) {
        visitDefault();
        return null;
    }

    @Override
    public Void visit(final SqlPredicateLess sqlPredicateLess) {
        visitDefault();
        return null;
    }

    @Override
    public Void visit(final SqlPredicateLessEqual sqlPredicateLessEqual) {
        visitDefault();
        return null;
    }

    @Override
    public Void visit(final SqlPredicateLike sqlPredicateLike) {
        visitDefault();
        return null;
    }

    @Override
    public Void visit(final SqlPredicateLikeRegexp sqlPredicateLikeRegexp) {
        visitDefault();
        return null;
    }

    @Override
    public Void visit(final SqlPredicateNot sqlPredicateNot) {
        visitDefault();
        return null;
    }

    @Override
    public Void visit(final SqlPredicateNotEqual sqlPredicateNotEqual) {
        visitDefault();
        return null;
    }

    @Override
    public Void visit(final SqlPredicateOr sqlPredicateOr) {
        visitDefault();
        return null;
    }

    @Override
    public Void visit(final SqlPredicateIsNotNull sqlPredicateOr) {
        visitDefault();
        return null;
    }

    @Override
    public Void visit(final SqlPredicateIsNull sqlPredicateOr) {
        visitDefault();
        return null;
    }

    @Override
    public Void visit(final SqlTable sqlTable) {
        visitDefault();
        return null;
    }

    @Override
    public Void visit(final SqlJoin sqlJoin) {
        visitDefault();
        return null;
    }

    public void visitDefault() {
        throw new UnsupportedOperationException("A handling for this SQL statement part was not implemented yet.");
    }
}
