package com.exasol.adapter.dynamodb.mapping;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathWalkerException;
import com.exasol.adapter.dynamodb.documentpath.PathIterationStateProvider;
import com.exasol.sql.expression.ValueExpression;

/**
 * Interface for extracting a value specified in a {@link ColumnMapping} from a document.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public interface ColumnValueExtractor<DocumentVisitorType> {

    /**
     * Extracts the columns values from the given document.
     *
     * @param document               to extract the value from
     * @param arrayAllIterationState array all iteration state used for extracting the correct values for nested lists
     * @return {@link ValueExpression}
     * @throws DocumentPathWalkerException   if specified property was not found and {@link LookupFailBehaviour} is set
     *                                       to {@code EXCEPTION }
     * @throws ColumnValueExtractorException if specified property can't be mapped and {@link LookupFailBehaviour} is
     *                                       set to {@code EXCEPTION }
     */
    public ValueExpression extractColumnValue(final DocumentNode<DocumentVisitorType> document,
            final PathIterationStateProvider arrayAllIterationState);
}
