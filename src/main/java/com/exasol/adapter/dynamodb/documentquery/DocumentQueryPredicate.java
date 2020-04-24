package com.exasol.adapter.dynamodb.documentquery;

import java.io.Serializable;

/**
 * This interface represents a selection predicate. Using the classes implementing this interface a where clause are
 * modeled.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public interface DocumentQueryPredicate<DocumentVisitorType> extends Serializable {
    void accept(QueryPredicateVisitor<DocumentVisitorType> visitor);
}
