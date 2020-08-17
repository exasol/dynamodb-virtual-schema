package com.exasol.adapter.document.mapping;

import java.math.BigDecimal;
import java.math.RoundingMode;

import com.exasol.adapter.document.documentnode.DocumentNode;
import com.exasol.sql.expression.BigDecimalLiteral;
import com.exasol.sql.expression.NullLiteral;
import com.exasol.sql.expression.ValueExpression;

/**
 * This class extracts DECIMAL values from document data. The extraction is defined using a
 * {@link PropertyToDecimalColumnMapping}.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public abstract class PropertyToDecimalColumnValueExtractor<DocumentVisitorType>
        extends AbstractPropertyToColumnValueExtractor<DocumentVisitorType> {
    private final PropertyToDecimalColumnMapping column;

    /**
     * Create an instance of {@link PropertyToDecimalColumnValueExtractor}.
     *
     * @param column {@link PropertyToDecimalColumnMapping} defining the mapping
     */
    public PropertyToDecimalColumnValueExtractor(final PropertyToDecimalColumnMapping column) {
        super(column);
        this.column = column;
    }

    @Override
    protected final ValueExpression mapValue(final DocumentNode<DocumentVisitorType> documentValue) {
        final BigDecimal decimalValue = mapValueToDecimalWithExceptionHandling(documentValue);
        if (decimalValue == null) {
            return handleNotANumber();
        } else {
            return fitValue(decimalValue);
        }
    }

    private ValueExpression fitValue(final BigDecimal decimalValue) {
        final BigDecimal decimalWithDestinationScale = decimalValue.setScale(this.column.getDecimalScale(),
                RoundingMode.FLOOR);
        if (decimalWithDestinationScale.precision() > this.column.getDecimalPrecision()) {
            return handleOverflow();
        } else {
            return BigDecimalLiteral.of(decimalWithDestinationScale);
        }
    }

    private BigDecimal mapValueToDecimalWithExceptionHandling(final DocumentNode<DocumentVisitorType> documentValue) {
        try {
            return mapValueToDecimal(documentValue);
        } catch (final NumberFormatException exception) {
            throw new ColumnValueExtractorException(
                    "Could not read map the data source's number. Cause: " + exception.getMessage(), exception,
                    this.column);
        }
    }

    private ValueExpression handleNotANumber() {
        if (this.column.getNotNumericBehaviour() == MappingErrorBehaviour.ABORT) {
            throw new ColumnValueExtractorException(
                    "The input value was no number. Try using a different mapping or ignore this error by setting notNumericBehaviour = \"null\".",
                    this.column);
        } else {
            return NullLiteral.nullLiteral();
        }
    }

    private ValueExpression handleOverflow() {
        if (this.column.getOverflowBehaviour() == MappingErrorBehaviour.ABORT) {
            throw new OverflowException("The input value exceeded the size of the " + this.column.getExasolColumnName()
                    + " DECIMAL column."
                    + " You can either increase the DECIMAL precision of this column or set the overflow behaviour to NULL.",
                    this.column);
        } else {
            return NullLiteral.nullLiteral();
        }
    }

    /**
     * Convert the document value to a BigDecimal. If not a number return {@code null}.
     * 
     * @param documentValue document value to convert
     * @return BigDecimal representation
     */
    protected abstract BigDecimal mapValueToDecimal(final DocumentNode<DocumentVisitorType> documentValue);
}
