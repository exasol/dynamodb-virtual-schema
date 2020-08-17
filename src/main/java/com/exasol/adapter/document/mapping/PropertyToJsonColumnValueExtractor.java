package com.exasol.adapter.document.mapping;

import com.exasol.adapter.document.documentnode.DocumentNode;
import com.exasol.sql.expression.NullLiteral;
import com.exasol.sql.expression.StringLiteral;
import com.exasol.sql.expression.ValueExpression;

/**
 * {@link ColumnValueExtractor} for {@link PropertyToJsonColumnMapping}.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public abstract class PropertyToJsonColumnValueExtractor<DocumentVisitorType>
        extends AbstractPropertyToColumnValueExtractor<DocumentVisitorType> {
    private final PropertyToJsonColumnMapping column;

    /**
     * Create an instance of {@link PropertyToJsonColumnValueExtractor}.
     * 
     * @param column {@link PropertyToJsonColumnMapping}
     */
    public PropertyToJsonColumnValueExtractor(final PropertyToJsonColumnMapping column) {
        super(column);
        this.column = column;
    }

    @Override
    protected final ValueExpression mapValue(final DocumentNode<DocumentVisitorType> documentValue) {
        final String jsonValue = mapJsonValue(documentValue);
        if (jsonValue.length() > this.column.getVarcharColumnSize()) {
            if (this.column.getOverflowBehaviour().equals(MappingErrorBehaviour.ABORT)) {
                throw new OverflowException(
                        "The generated JSON did exceed the configured maximum size. You can either increase the column size of this column or set the overflow behaviour to NULL.",
                        this.column);
            } else {
                return NullLiteral.nullLiteral();
            }
        } else {
            return StringLiteral.of(jsonValue);
        }
    }

    protected abstract String mapJsonValue(final DocumentNode<DocumentVisitorType> dynamodbProperty);
}
