package com.exasol.adapter.dynamodb.mapping;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.sql.expression.StringLiteral;
import com.exasol.sql.expression.ValueExpression;

/**
 * ValueMapper for {@link ToStringPropertyToColumnMapping}
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public abstract class ToStringPropertyToColumnValueExtractor<DocumentVisitorType>
        extends AbstractPropertyToColumnValueExtractor<DocumentVisitorType> {
    private final ToStringPropertyToColumnMapping column;

    /**
     * Create an instance of {@link ToStringPropertyToColumnValueExtractor}.
     * 
     * @param column {@link ToStringPropertyToColumnMapping}
     */
    public ToStringPropertyToColumnValueExtractor(final ToStringPropertyToColumnMapping column) {
        super(column);
        this.column = column;
    }

    @Override
    protected ValueExpression mapValue(final DocumentNode<DocumentVisitorType> documentValue) {
        final String stringValue = mapStringValue(documentValue);
        if (stringValue == null) {
            return this.column.getExasolDefaultValue();
        } else {
            return StringLiteral.of(handleOverflowIfNecessary(stringValue));
        }
    }

    protected abstract String mapStringValue(DocumentNode<DocumentVisitorType> dynamodbProperty);

    private String handleOverflowIfNecessary(final String sourceString) {
        if (sourceString.length() > this.column.getVarcharColumnSize()) {
            return handleOverflow(sourceString);
        } else {
            return sourceString;
        }
    }

    private String handleOverflow(final String tooLongSourceString) {
        if (this.column.getOverflowBehaviour() == ToStringPropertyToColumnMapping.OverflowBehaviour.TRUNCATE) {
            return tooLongSourceString.substring(0, this.column.getVarcharColumnSize());
        } else {
            throw new OverflowException(
                    "String overflow. You can either increase the string size if this column or set the overflow behaviour to truncate.",
                    this.column);
        }
    }

}
