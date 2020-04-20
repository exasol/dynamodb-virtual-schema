package com.exasol.adapter.dynamodb.mapping;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathWalkerException;
import com.exasol.adapter.dynamodb.documentpath.LinearDocumentPathWalker;
import com.exasol.sql.expression.ValueExpression;

/**
 * Abstract class for extracting a value specified in an ColumnMappingDefinition from a DynamoDB row.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public abstract class AbstractValueMapper<DocumentVisitorType> {
    private final AbstractColumnMappingDefinition column;

    /**
     * Creates an instance of {@link AbstractValueMapper} for extracting a value specified parameter column from a
     * DynamoDB row.
     * 
     * @param column ColumnMappingDefinition defining the mapping
     */
    public AbstractValueMapper(final AbstractColumnMappingDefinition column) {
        this.column = column;
    }

    /**
     * Extracts {@link #column}s value from DynamoDB's result row.
     *
     * @param dynamodbRow to extract the value from
     * @return {@link ValueExpression}
     * @throws DocumentPathWalkerException if specified property was not found and
     *                                     {@link AbstractColumnMappingDefinition.LookupFailBehaviour} is set to
     *                                     {@code EXCEPTION }
     * @throws ValueMapperException        if specified property can't be mapped and
     *                                     {@link AbstractColumnMappingDefinition.LookupFailBehaviour} is set to
     *                                     {@code EXCEPTION }
     */
    public ValueExpression mapRow(final DocumentNode<DocumentVisitorType> dynamodbRow) {
        try {
            final LinearDocumentPathWalker<DocumentVisitorType> walker = new LinearDocumentPathWalker<>(
                    this.column.getPathToSourceProperty());
            final DocumentNode<DocumentVisitorType> dynamodbProperty = walker.walkThroughDocument(dynamodbRow);
            return mapValue(dynamodbProperty);
        } catch (final DocumentPathWalkerException | LookupValueMapperException exception) {
            if (this.column
                    .getLookupFailBehaviour() == AbstractColumnMappingDefinition.LookupFailBehaviour.DEFAULT_VALUE) {
                return this.column.getExasolDefaultValue();
            } else {
                throw exception;
            }
        }
    }

    /**
     * Converts the DynamoDB property into an Exasol {@link ValueExpression}.
     *
     * @param dynamodbProperty the DynamoDB property to be converted
     * @return the conversion result
     * @throws ValueMapperException if the value can't be mapped
     */
    protected abstract ValueExpression mapValue(DocumentNode<DocumentVisitorType> dynamodbProperty);
}
