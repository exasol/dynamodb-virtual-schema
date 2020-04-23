package com.exasol.adapter.dynamodb.queryplan;

import java.util.List;

/**
 * This class represents a {@code AND} predicate.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public class AndPredicate<DocumentVisitorType> implements DocumentQueryPredicate<DocumentVisitorType> {
    private static final long serialVersionUID = -8351558984178219419L;
    private final List<DocumentQueryPredicate<DocumentVisitorType>> andPredicates;

    /**
     * Creates new instance of {@link AndPredicate}.
     *
     * @param andedPredicates the anded predicates
     */
    public AndPredicate(final List<DocumentQueryPredicate<DocumentVisitorType>> andedPredicates) {
        this.andPredicates = andedPredicates;
    }

    /**
     * Gives the anded predicates.
     * 
     * @return list of anded predicates.
     */
    public List<DocumentQueryPredicate<DocumentVisitorType>> getAndPredicates() {
        return this.andPredicates;
    }

    @Override
    public void accept(final QueryPredicateVisitor<DocumentVisitorType> visitor) {
        visitor.visit(this);
    }
}
