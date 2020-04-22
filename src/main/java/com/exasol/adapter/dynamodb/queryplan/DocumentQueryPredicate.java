package com.exasol.adapter.dynamodb.queryplan;

import java.io.Serializable;

public interface DocumentQueryPredicate<DocumentVisitorType> extends Serializable {
    void accept(QueryPredicateVisitor<DocumentVisitorType> visitor);
}
