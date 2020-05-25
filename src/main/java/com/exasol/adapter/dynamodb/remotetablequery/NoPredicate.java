package com.exasol.adapter.dynamodb.remotetablequery;

/**
 * This class represents the absence of a selection predicate.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public class NoPredicate<DocumentVisitorType> implements QueryPredicate<DocumentVisitorType> {
    private static final long serialVersionUID = -7964488054466482230L;

    @Override
    public void accept(final QueryPredicateVisitor<DocumentVisitorType> visitor) {
        visitor.visit(this);
    }

    @Override
    public boolean equals(final Object other) {
        if (other == null) {
            return false;
        }
        return this.getClass().equals(other.getClass());
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
