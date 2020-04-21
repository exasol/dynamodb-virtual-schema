package com.exasol.adapter.dynamodb.queryrunner;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbTableMetadata;
import com.exasol.adapter.dynamodb.literalconverter.NotALiteralException;
import com.exasol.adapter.dynamodb.literalconverter.SqlToDynamodbLiteralConverter;
import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.SchemaMappingDefinitionToSchemaMetadataConverter;
import com.exasol.adapter.sql.*;

public class DynamodbGetItemQueryPlanFactory {

    public DynamodbGetItemQueryPlan buildGetItemPlanIfPossible(final String tableName, final SqlStatement query,
            final DynamodbTableMetadata tableMetadata) throws PlanDoesNotFitException {
        final Visitor visitor = new Visitor(tableMetadata);
        try {
            query.accept(visitor);
            final Map<String, AttributeValue> key = visitor.getPrimaryKey();
            checkIfKeyIsComplete(key, tableMetadata);
            return new DynamodbGetItemQueryPlan(tableName, key);
        } catch (final AdapterException exception) {
            // This should never happen, as we do not throw adapter exceptions in the visitor.
            throw new IllegalStateException("An unexpected adapter exception occurred", exception);
        } catch (final PlanDoesNotFitExceptionWrapper wrapper) {
            throw wrapper.getWrappedException();
        }
    }

    private void checkIfKeyIsComplete(final Map<String, AttributeValue> key, final DynamodbTableMetadata tableMetadata)
            throws PlanDoesNotFitException {
        if (key.size() < 1) {
            throw new PlanDoesNotFitException(
                    "Not a GetItem request as the partition key was not specified in the where clause.");
        }
        if (tableMetadata.getPrimaryKey().hasSortKey() && key.size() != 2) {
            throw new PlanDoesNotFitException(
                    "Not a GetItem request as the sort key was not specified in the where clause.");
        }
    }

    private static class Visitor extends VoidSqlNodeVisitor {
        private final DynamodbTableMetadata tableMetadata;
        private final Map<String, AttributeValue> primaryKey = new HashMap<>();

        private Visitor(final DynamodbTableMetadata tableMetadata) {
            this.tableMetadata = tableMetadata;
        }

        @Override
        public Void visit(final SqlStatementSelect select) throws AdapterException {
            if (select.getWhereClause() != null) {
                select.getWhereClause().accept(this);
                return null;
            } else {
                throw new PlanDoesNotFitExceptionWrapper(new PlanDoesNotFitException(
                        "This is not an getItem request as the query has no where clause and so no selection."));
            }
        }

        @Override
        public Void visit(final SqlPredicateEqual sqlPredicateEqual) {
            sqlPredicateEqual.getLeft().getType();
            final SqlNode left = sqlPredicateEqual.getLeft();
            final SqlNode right = sqlPredicateEqual.getRight();
            if (left instanceof SqlColumn) {
                tryToExtractKey((SqlColumn) left, right);
            } else {
                tryToExtractKey((SqlColumn) right, left);
            }
            return null;
        }

        void tryToExtractKey(final SqlColumn column, final SqlNode literal) {
            try {
                final AttributeValue literalValue = new SqlToDynamodbLiteralConverter().convert(literal);
                final AbstractColumnMappingDefinition columnMapping = new SchemaMappingDefinitionToSchemaMetadataConverter()
                        .convertBackColumn(column.getMetadata());
                final DocumentPathExpression columnPath = columnMapping.getPathToSourceProperty();
                tryToAddKey(this.tableMetadata.getPrimaryKey().getPartitionKey(), columnPath, literalValue);
                tryToAddKey(this.tableMetadata.getPrimaryKey().getSortKey(), columnPath, literalValue);
            } catch (final NotALiteralException exception) {
                throw new PlanDoesNotFitExceptionWrapper(new PlanDoesNotFitException(
                        "This is not a getItem request as it contains equality predicates on non literals."));
            }
        }

        void tryToAddKey(final String key, final DocumentPathExpression columnPath, final AttributeValue value) {
            if (columnPath.equals(new DocumentPathExpression.Builder().addObjectLookup(key).build())) {
                if (this.primaryKey.containsKey(key)) {
                    if (this.primaryKey.get(key).equals(value)) {
                        return; // duplicate condition. We just skip this key.
                    } else {
                        throw new PlanDoesNotFitExceptionWrapper(new PlanDoesNotFitException(
                                "This is not a getItem request as the same key is restricted in the where clause twice."));
                    }
                }
                this.primaryKey.put(key, value);
            }
        }

        @Override
        public Void visit(final SqlPredicateAnd sqlPredicateAnd) throws AdapterException {
            for (final SqlNode andPredicate : sqlPredicateAnd.getAndedPredicates()) {
                andPredicate.accept(this);
            }
            return null;
        }

        public Map<String, AttributeValue> getPrimaryKey() {
            return this.primaryKey;
        }

        @Override
        public void visitDefault() {
            throw new PlanDoesNotFitExceptionWrapper(
                    new PlanDoesNotFitException("This predicate is not supported for GetItem requests."));
        }
    }

}
