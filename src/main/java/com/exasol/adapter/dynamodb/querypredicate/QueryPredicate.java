package com.exasol.adapter.dynamodb.querypredicate;

import java.io.Serializable;

/**
 * This interface represents a selection predicate. Using the classes implementing this interface a where clause are
 * modeled.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public interface QueryPredicate extends Serializable {
    public void accept(QueryPredicateVisitor visitor);

    /**
     * Eliminate ANDs and ORs with only no or only one operand.
     * 
     * @return simplified predicate.
     */
    public QueryPredicate simplify();
}
