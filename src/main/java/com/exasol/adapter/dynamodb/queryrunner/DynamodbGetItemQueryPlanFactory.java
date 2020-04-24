package com.exasol.adapter.dynamodb.queryrunner;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeToAttributeValueConverter;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.dynamodb.documentquery.*;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbTableMetadata;

/**
 * This class builds a {@link DynamodbGetItemQueryPlan} if possible.
 */
public class DynamodbGetItemQueryPlanFactory {

    /**
     * Builds a {@link DynamodbGetItemQueryPlan} is possible for the given query.
     * 
     * @param documentQuery query to build the plan for
     * @param tableMetadata DynamoDB table metadata used for checking the primary key
     * @return the generated plan
     * @throws PlanDoesNotFitException if this query can't be executed using a {@link DynamodbGetItemQueryPlan}
     */
    public DynamodbGetItemQueryPlan buildGetItemPlanIfPossible(final DocumentQuery<DynamodbNodeVisitor> documentQuery,
            final DynamodbTableMetadata tableMetadata) throws PlanDoesNotFitException {
        final Visitor visitor = new Visitor(tableMetadata);
        try {
            documentQuery.getSelection().accept(visitor);
            final Map<String, AttributeValue> key = visitor.getPrimaryKey();
            checkIfKeyIsComplete(key, tableMetadata);
            return new DynamodbGetItemQueryPlan(documentQuery.getFromTable().getRemoteName(), key);
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

    private static class Visitor implements QueryPredicateVisitor<DynamodbNodeVisitor> {
        private final DynamodbTableMetadata tableMetadata;
        private final Map<String, AttributeValue> primaryKey = new HashMap<>();

        private Visitor(final DynamodbTableMetadata tableMetadata) {
            this.tableMetadata = tableMetadata;
        }

        @Override
        public void visit(
                final ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> columnLiteralComparisonPredicate) {
            final DocumentPathExpression columnPath = columnLiteralComparisonPredicate.getColumn()
                    .getPathToSourceProperty();
            if (columnPath.size() != 1) {
                return; // This is not an key attribute as it
            }
            final AttributeValue attributeValue = new DynamodbNodeToAttributeValueConverter()
                    .convertToAttributeValue(columnLiteralComparisonPredicate.getLiteral());
            tryToAddKey(this.tableMetadata.getPrimaryKey().getPartitionKey(), columnPath, attributeValue);
            tryToAddKey(this.tableMetadata.getPrimaryKey().getSortKey(), columnPath, attributeValue);
        }

        private void tryToAddKey(final String key, final DocumentPathExpression columnPath,
                final AttributeValue value) {
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
        public void visit(final BinaryLogicalOperator<DynamodbNodeVisitor> binaryLogicalOperator) {
            switch (binaryLogicalOperator.getOperator()) {
            case AND:
                for (final DocumentQueryPredicate<DynamodbNodeVisitor> andedPredicate : binaryLogicalOperator
                        .getOperands()) {
                    andedPredicate.accept(this);
                }
                break;
            case OR:
                throw new PlanDoesNotFitExceptionWrapper(
                        new PlanDoesNotFitException("OR operators are not supported for GetItem requests."));
            default:
                break;
            }
        }

        @Override
        public void visit(final NoPredicate<DynamodbNodeVisitor> noPredicate) {
            // if no selection is done we don't add a key.
        }

        private Map<String, AttributeValue> getPrimaryKey() {
            return this.primaryKey;
        }
    }
}
