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
}
