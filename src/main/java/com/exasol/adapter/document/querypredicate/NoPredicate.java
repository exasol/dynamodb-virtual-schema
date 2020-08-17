package com.exasol.adapter.document.querypredicate;

/**
 * This class represents the absence of a selection predicate.
 */
public final class NoPredicate implements QueryPredicate {
    private static final long serialVersionUID = -7964488054466482230L;

    @Override
    public void accept(final QueryPredicateVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public QueryPredicate simplify() {
        return this;
    }

    @Override
    public String toString() {
        return "NoPredicate";
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof NoPredicate;
    }

    @Override
    public int hashCode() {
        /*
         * Get a static value as all instances of this class are equal. The use of a single instance like * {@link
         * List#of()} is not possible due to generic type.
         */
        return 1;
    }
}
