package com.exasol.adapter.dynamodb.queryplan;

import java.util.List;
import java.util.stream.Collectors;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dynamodb.documentnode.DocumentValue;
import com.exasol.adapter.dynamodb.literalconverter.NotALiteralException;
import com.exasol.adapter.dynamodb.literalconverter.SqlLiteralToDocumentValueConverter;
import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.SchemaMappingDefinitionToSchemaMetadataConverter;
import com.exasol.adapter.sql.*;

public class DocumentQueryPredicateFactory<DocumentVisitorType> {
    private final SqlLiteralToDocumentValueConverter<DocumentVisitorType> literalConverter;

    public DocumentQueryPredicateFactory(
            final SqlLiteralToDocumentValueConverter<DocumentVisitorType> literalConverter) {
        this.literalConverter = literalConverter;
    }

    public DocumentQueryPredicate<DocumentVisitorType> buildPredicateFor(final SqlNode sqlPredicate) {
        final Visitor visitor = new Visitor();
        if (sqlPredicate == null) {
            return new NoPredicate<>();
        } else {
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
        private DocumentQueryPredicate<DocumentVisitorType> predicate;

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
                final AbstractColumnMappingDefinition columnMapping = new SchemaMappingDefinitionToSchemaMetadataConverter()
                        .convertBackColumn(column.getMetadata());
                final DocumentValue<DocumentVisitorType> literalValue = DocumentQueryPredicateFactory.this.literalConverter
                        .convert(literal);
                this.predicate = new ColumnLiteralComparisonPredicate<>(operator, columnMapping, literalValue);
            } catch (final NotALiteralException e) {
                throw new IllegalStateException(
                        "This predicate or function is not supported in this version of this Virtual Schema.");
            }
        }

        @Override
        public Void visit(final SqlPredicateAnd sqlPredicateAnd) throws AdapterException {
            this.predicate = new AndPredicate<>(convertPredicates(sqlPredicateAnd.getAndedPredicates()));
            return null;
        }

        @Override
        public Void visit(final SqlPredicateOr sqlPredicateOr) {
            this.predicate = new OrPredicate<>(convertPredicates(sqlPredicateOr.getOrPredicates()));
            return null;
        }

        private List<DocumentQueryPredicate<DocumentVisitorType>> convertPredicates(final List<SqlNode> sqlPredicates) {
            final DocumentQueryPredicateFactory<DocumentVisitorType> factory = new DocumentQueryPredicateFactory<>(
                    DocumentQueryPredicateFactory.this.literalConverter);
            return sqlPredicates.stream().map(factory::buildPredicateFor).collect(Collectors.toList());
        }

        private DocumentQueryPredicate<DocumentVisitorType> getPredicate() {
            return this.predicate;
        }
    }
}
