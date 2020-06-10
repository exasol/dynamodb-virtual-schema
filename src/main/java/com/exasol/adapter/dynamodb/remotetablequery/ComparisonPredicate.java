package com.exasol.adapter.dynamodb.remotetablequery;

import java.util.List;

import com.exasol.adapter.dynamodb.mapping.ColumnMapping;

/**
 * This interface represents a comparison between a literal and a column of a table.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public interface ComparisonPredicate<DocumentVisitorType> extends QueryPredicate<DocumentVisitorType> {

    /**
     * Get the comparison operator.
     *
     * @return the operator
     */
    public AbstractComparisonPredicate.Operator getOperator();

    /**
     * Accept an {@link ComparisonPredicateVisitor}.
     */
    public void accept(ComparisonPredicateVisitor<DocumentVisitorType> visitor);

    @Override
    public default void accept(final QueryPredicateVisitor<DocumentVisitorType> visitor) {
        visitor.visit(this);
    }

    /**
     * Get a list of {@link ColumnMapping}s involved in the comparison.
     */
    public List<ColumnMapping> getComparedColumns();

    /**
     * Negates this operator. e.g. A = B --> A != B
     */
    public ComparisonPredicate<DocumentVisitorType> negate();
}
