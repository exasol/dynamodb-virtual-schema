package com.exasol.adapter.dynamodb.queryplanning;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.dynamodb.mapping.ColumnMapping;
import com.exasol.adapter.dynamodb.mapping.ColumnMappingVisitor;
import com.exasol.adapter.dynamodb.mapping.IterationIndexColumnMapping;
import com.exasol.adapter.dynamodb.mapping.PropertyToColumnMapping;
import com.exasol.adapter.dynamodb.querypredicate.InvolvedColumnCollector;

/**
 * This class extracts the required {@link DocumentPathExpression}s that must be fetched from the remote database for
 * solving the given query.
 */
public class RequiredPathExpressionExtractor {

    /**
     * Get a set of properties that must be fetched from the remote database.
     * 
     * @return set of required properties
     */
    public Set<DocumentPathExpression> getRequiredProperties(final RemoteTableQuery query) {
        final List<ColumnMapping> requiredColumns = new ArrayList<>(
                new InvolvedColumnCollector().collectInvolvedColumns(query.getPostSelection()));
        requiredColumns.addAll(query.getSelectList());
        return requiredColumns.stream().map(this::getRequiredProperty).collect(Collectors.toSet());
    }

    private DocumentPathExpression getRequiredProperty(final ColumnMapping columnMapping) {
        final Visitor visitor = new Visitor();
        columnMapping.accept(visitor);
        return visitor.requiredPathExpression;
    }

    private static class Visitor implements ColumnMappingVisitor {
        private DocumentPathExpression requiredPathExpression;

        @Override
        public void visit(final PropertyToColumnMapping propertyToColumnMapping) {
            this.requiredPathExpression = propertyToColumnMapping.getPathToSourceProperty();
        }

        @Override
        public void visit(final IterationIndexColumnMapping iterationIndexColumnDefinition) {
            this.requiredPathExpression = iterationIndexColumnDefinition.getTablesPath();
        }
    }
}
