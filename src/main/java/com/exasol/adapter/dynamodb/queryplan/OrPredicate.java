package com.exasol.adapter.dynamodb.queryplan;

import java.util.List;

/**
 * This class represents a {@code OR} predicate.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public class OrPredicate<DocumentVisitorType> implements DocumentQueryPredicate<DocumentVisitorType> {
    private static final long serialVersionUID = 7098084004613237441L;
    private final List<DocumentQueryPredicate<DocumentVisitorType>> oredPredicates;

    /**
     * Creates an instance of {@link OrPredicate}.
     * 
     * @param oredPredicates predicates that are ored.
     */
    public OrPredicate(final List<DocumentQueryPredicate<DocumentVisitorType>> oredPredicates) {
        this.oredPredicates = oredPredicates;
    }

    /**
     * Gives the ored predicates.
     * 
     * @return ored predicates
     */
    public List<DocumentQueryPredicate<DocumentVisitorType>> getOredPredicates() {
        return this.oredPredicates;
    }

    @Override
    public void accept(final QueryPredicateVisitor<DocumentVisitorType> visitor) {
        visitor.visit(this);
    }
}
