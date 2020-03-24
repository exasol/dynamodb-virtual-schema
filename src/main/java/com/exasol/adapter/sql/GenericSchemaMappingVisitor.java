package com.exasol.adapter.sql;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dynamodb.queryresult.QueryResultTableBuilder;

/**
 * Facade for the {@link SqlNodeVisitor} interface implementing all methods with
 * unsupported exception. This class is used for keeping
 * {@link QueryResultTableBuilder} short and readable.
 */
public abstract class GenericSchemaMappingVisitor implements SqlNodeVisitor<Void> {
	private static final String UNSUPPORTED_MESSAGE = "not yet supported";
	@Override
	public Void visit(final SqlSelectList selectList) throws AdapterException {
		throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
	}

	@Override
	public Void visit(final SqlGroupBy groupBy) {
		throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
	}

	@Override
	public Void visit(final SqlColumn sqlColumn) {
		throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
	}

	@Override
	public Void visit(final SqlFunctionAggregate sqlFunctionAggregate) {
		throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
	}

	@Override
	public Void visit(final SqlFunctionAggregateGroupConcat sqlFunctionAggregateGroupConcat) {
		throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
	}

	@Override
	public Void visit(final SqlFunctionScalar sqlFunctionScalar) {
		throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
	}

	@Override
	public Void visit(final SqlFunctionScalarCase sqlFunctionScalarCase) {
		throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
	}

	@Override
	public Void visit(final SqlFunctionScalarCast sqlFunctionScalarCast) {
		throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
	}

	@Override
	public Void visit(final SqlFunctionScalarExtract sqlFunctionScalarExtract) {
		throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
	}

	@Override
	public Void visit(final SqlLimit sqlLimit) {
		throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
	}

	@Override
	public Void visit(final SqlLiteralBool sqlLiteralBool) {
		throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
	}

	@Override
	public Void visit(final SqlLiteralDate sqlLiteralDate) {
		throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
	}

	@Override
	public Void visit(final SqlLiteralDouble sqlLiteralDouble) {
		throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
	}

	@Override
	public Void visit(final SqlLiteralExactnumeric sqlLiteralExactnumeric) {
		throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
	}

	@Override
	public Void visit(final SqlLiteralNull sqlLiteralNull) {
		throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
	}

	@Override
	public Void visit(final SqlLiteralString sqlLiteralString) {
		throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
	}

	@Override
	public Void visit(final SqlLiteralTimestamp sqlLiteralTimestamp) {
		throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
	}

	@Override
	public Void visit(final SqlLiteralTimestampUtc sqlLiteralTimestampUtc) {
		throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
	}

	@Override
	public Void visit(final SqlLiteralInterval sqlLiteralInterval) {
		throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
	}

	@Override
	public Void visit(final SqlOrderBy sqlOrderBy) {
		throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
	}

	@Override
	public Void visit(final SqlPredicateAnd sqlPredicateAnd) {
		throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
	}

	@Override
	public Void visit(final SqlPredicateBetween sqlPredicateBetween) {
		throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
	}

	@Override
	public Void visit(final SqlPredicateEqual sqlPredicateEqual) {
		throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
	}

	@Override
	public Void visit(final SqlPredicateInConstList sqlPredicateInConstList) {
		throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
	}

	@Override
	public Void visit(final SqlPredicateLess sqlPredicateLess) {
		throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
	}

	@Override
	public Void visit(final SqlPredicateLessEqual sqlPredicateLessEqual) {
		throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
	}

	@Override
	public Void visit(final SqlPredicateLike sqlPredicateLike) {
		throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
	}

	@Override
	public Void visit(final SqlPredicateLikeRegexp sqlPredicateLikeRegexp) {
		throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
	}

	@Override
	public Void visit(final SqlPredicateNot sqlPredicateNot) {
		throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
	}

	@Override
	public Void visit(final SqlPredicateNotEqual sqlPredicateNotEqual) {
		throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
	}

	@Override
	public Void visit(final SqlPredicateOr sqlPredicateOr) {
		throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
	}

	@Override
	public Void visit(final SqlPredicateIsNotNull sqlPredicateOr) {
		throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
	}

	@Override
	public Void visit(final SqlPredicateIsNull sqlPredicateOr) {
		throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
	}

	@Override
	public Void visit(final SqlTable sqlTable) {
		throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
	}

	@Override
	public Void visit(final SqlJoin sqlJoin) {
		throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
	}
}
