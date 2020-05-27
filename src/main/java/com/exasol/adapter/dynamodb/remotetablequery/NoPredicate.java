package com.exasol.adapter.dynamodb.remotetablequery;

/**
 * This class represents the absence of a selection predicate.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public final class NoPredicate<DocumentVisitorType> implements QueryPredicate<DocumentVisitorType> {
    private static final long serialVersionUID = -7964488054466482230L;

    @Override
    public void accept(final QueryPredicateVisitor<DocumentVisitorType> visitor) {
        visitor.visit(this);
    }

    @Override
    public QueryPredicate<DocumentVisitorType> simplify() {
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
