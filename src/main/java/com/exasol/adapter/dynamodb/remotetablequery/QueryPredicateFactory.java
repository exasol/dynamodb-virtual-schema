package com.exasol.adapter.dynamodb.remotetablequery;

import java.util.List;
import java.util.stream.Collectors;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dynamodb.documentnode.DocumentValue;
import com.exasol.adapter.dynamodb.literalconverter.NotLiteralException;
import com.exasol.adapter.dynamodb.literalconverter.SqlLiteralToDocumentValueConverter;
import com.exasol.adapter.dynamodb.mapping.ColumnMapping;
import com.exasol.adapter.dynamodb.mapping.SchemaMappingToSchemaMetadataConverter;
import com.exasol.adapter.sql.*;

/**
 * This class builds a{@link QueryPredicate} structure from a {@link SqlStatementSelect}s where clause. The new
 * structure represents the same conditional logic but uses deserialized column definitions, literals of the remote
 * database and is serializable.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public class QueryPredicateFactory<DocumentVisitorType> {
    private final SqlLiteralToDocumentValueConverter<DocumentVisitorType> literalConverter;

    /**
     * Creates an instance of {@link QueryPredicateFactory}.
     * 
     * @param literalConverter implementation specific converter for literals
     */
    public QueryPredicateFactory(final SqlLiteralToDocumentValueConverter<DocumentVisitorType> literalConverter) {
        this.literalConverter = literalConverter;
    }

    /**
     * Converts the given SQL predicate into a {@link QueryPredicate} structure.
     * 
     * @param sqlPredicate SQL predicate to convert
     * @return {@link QueryPredicate} structure
     */
    public QueryPredicate<DocumentVisitorType> buildPredicateFor(final SqlNode sqlPredicate) {
        if (sqlPredicate == null) {
            return new NoPredicate<>();
        } else {
            final Visitor visitor = new Visitor();
            try {
                sqlPredicate.accept(visitor);
            } catch (final AdapterException exception) {
                // This should never happen, as we do not throw adapter exceptions in the visitor.
                throw new IllegalStateException("An unexpected adapter exception occurred", exception);
            }
            return visitor.getPredicate();
        }
    }

    private class Visitor extends VoidSqlNodeVisitor {
        private QueryPredicate<DocumentVisitorType> predicate;

        @Override
        public Void visit(final SqlPredicateEqual sqlPredicateEqual) {
            buildComparison(sqlPredicateEqual, ComparisonPredicate.Operator.EQUAL);
            return null;
        }

        void buildComparison(final AbstractSqlBinaryEquality sqlEquality, final ComparisonPredicate.Operator operator) {
            final SqlNode left = sqlEquality.getLeft();
            final SqlNode right = sqlEquality.getRight();
            if (left instanceof SqlColumn && right instanceof SqlColumn) {
                throw new UnsupportedOperationException(
                        "Predicates on two columns are not yet supported in this Virtual Schema version.");
            } else if (right instanceof SqlColumn) {
                buildColumnLiteralComparision((SqlColumn) right, left, operator);
            } else if (left instanceof SqlColumn) {
                buildColumnLiteralComparision((SqlColumn) left, right, operator);
            } else {
                throw new UnsupportedOperationException(
                        "Predicates on two literals are not yet supported in this Virtual Schema version.");
            }
        }

        void buildColumnLiteralComparision(final SqlColumn column, final SqlNode literal,
                final ComparisonPredicate.Operator operator) {
            try {
                final ColumnMapping columnMapping = new SchemaMappingToSchemaMetadataConverter()
                        .convertBackColumn(column.getMetadata());
                final DocumentValue<DocumentVisitorType> literalValue = QueryPredicateFactory.this.literalConverter
                        .convert(literal);
                this.predicate = new ColumnLiteralComparisonPredicate<>(operator, columnMapping, literalValue);
            } catch (final NotLiteralException e) {
                throw new IllegalStateException(
                        "This predicate or function is not supported in this version of this Virtual Schema.");
            }
        }

        @Override
        public Void visit(final SqlPredicateAnd sqlPredicateAnd) {
            this.predicate = new LogicalOperator<>(convertPredicates(sqlPredicateAnd.getAndedPredicates()),
                    LogicalOperator.Operator.AND);
            return null;
        }

        @Override
        public Void visit(final SqlPredicateOr sqlPredicateOr) {
            this.predicate = new LogicalOperator<>(convertPredicates(sqlPredicateOr.getOrPredicates()),
                    LogicalOperator.Operator.OR);
            return null;
        }

        private List<QueryPredicate<DocumentVisitorType>> convertPredicates(final List<SqlNode> sqlPredicates) {
            final QueryPredicateFactory<DocumentVisitorType> factory = new QueryPredicateFactory<>(
                    QueryPredicateFactory.this.literalConverter);
            return sqlPredicates.stream().map(factory::buildPredicateFor).collect(Collectors.toList());
        }

        private QueryPredicate<DocumentVisitorType> getPredicate() {
            return this.predicate;
        }
    }
}
