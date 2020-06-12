package com.exasol.adapter.dynamodb.querypredicate;

import java.util.Objects;

/**
 * This class represents a {@code NOT}.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public final class NotPredicate<DocumentVisitorType> implements QueryPredicate<DocumentVisitorType> {
    private static final long serialVersionUID = -1358113690298643292L;
    private final QueryPredicate<DocumentVisitorType> predicate;

    public NotPredicate(final QueryPredicate<DocumentVisitorType> predicate) {
        this.predicate = predicate;
    }

    /**
     * Get the negated predicate
     * 
     * @return negated predicated
     */
    public QueryPredicate<DocumentVisitorType> getPredicate() {
        return this.predicate;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof NotPredicate)) {
            return false;
        }

        final NotPredicate<?> that = (NotPredicate<?>) other;
        return this.predicate.equals(that.predicate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.predicate.hashCode(), NotPredicate.class.getName());
    }

    @Override
    public String toString() {
        return "not( " + this.predicate.toString() + " )";
    }

    @Override
    public void accept(final QueryPredicateVisitor<DocumentVisitorType> visitor) {
        visitor.visit(this);
    }

    @Override
    public QueryPredicate<DocumentVisitorType> simplify() {
        return new NotPredicate<>(this.predicate.simplify());
    }
}
