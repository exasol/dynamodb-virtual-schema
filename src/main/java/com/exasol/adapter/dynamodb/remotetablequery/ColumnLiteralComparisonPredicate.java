package com.exasol.adapter.dynamodb.remotetablequery;

import com.exasol.adapter.dynamodb.documentnode.DocumentValue;
import com.exasol.adapter.dynamodb.mapping.ColumnMapping;

/**
 * This class represents a comparison between a literal and a column of a table.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public class ColumnLiteralComparisonPredicate<DocumentVisitorType> extends ComparisonPredicate<DocumentVisitorType> {
    private static final long serialVersionUID = 4471077828317147591L;
    private final DocumentValue<DocumentVisitorType> literal;
    private final ColumnMapping column;

    /**
     * Create an instance of {@link ColumnLiteralComparisonPredicate}.
     * 
     * @param operator comparison operator
     * @param column   column to compare
     * @param literal  literal to compare
     */
    public ColumnLiteralComparisonPredicate(final Operator operator, final ColumnMapping column,
            final DocumentValue<DocumentVisitorType> literal) {
        super(operator);
        this.literal = literal;
        this.column = column;
    }

    /**
     * Get the literal that the column is compared to in this predicate.
     * 
     * @return literal
     */
    public DocumentValue<DocumentVisitorType> getLiteral() {
        return this.literal;
    }

    /**
     * Get the column that the literal is compared to in this predicate.
     * 
     * @return column
     */
    public ColumnMapping getColumn() {
        return this.column;
    }

    @Override
    public void accept(final QueryPredicateVisitor<DocumentVisitorType> visitor) {
        visitor.visit(this);
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        final ColumnLiteralComparisonPredicate<?> that = (ColumnLiteralComparisonPredicate<?>) other;

        if (!this.literal.equals(that.literal)) {
            return false;
        }
        return this.column.equals(that.column);
    }

    @Override
    public int hashCode() {
        final int literalHash = this.literal.hashCode();
        return 31 * literalHash + this.column.hashCode();
    }

    @Override
    public String toString() {
        return this.column.getExasolColumnName() + super.toString() + this.literal.toString();
    }
}
