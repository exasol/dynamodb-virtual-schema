package com.exasol.adapter.document.querypredicate;

import java.util.Objects;

/**
 * This class represents a {@code NOT}.
 */
public final class NotPredicate implements QueryPredicate {
    private static final long serialVersionUID = -1358113690298643292L;
    private final QueryPredicate predicate;

    public NotPredicate(final QueryPredicate predicate) {
        this.predicate = predicate;
    }

    /**
     * Get the negated predicate
     * 
     * @return negated predicated
     */
    public QueryPredicate getPredicate() {
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

        final NotPredicate that = (NotPredicate) other;
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
    public void accept(final QueryPredicateVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public QueryPredicate simplify() {
        return new NotPredicate(this.predicate.simplify());
    }
}
