package com.exasol.adapter.document.querypredicate;

import java.util.List;

import com.exasol.adapter.document.mapping.ColumnMapping;

/**
 * This interface represents a comparison between a literal and a column of a table.
 */
public interface ComparisonPredicate extends QueryPredicate {

    /**
     * Get the comparison operator.
     *
     * @return the operator
     */
    public AbstractComparisonPredicate.Operator getOperator();

    /**
     * Accept an {@link ComparisonPredicateVisitor}.
     */
    public void accept(ComparisonPredicateVisitor visitor);

    @Override
    public default void accept(final QueryPredicateVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Get a list of {@link ColumnMapping}s involved in the comparison.
     */
    public List<ColumnMapping> getComparedColumns();

    /**
     * Negates this operator. e.g. A = B --> A != B
     */
    public ComparisonPredicate negate();
}
