package com.exasol.adapter.dynamodb.queryplan;

import java.util.List;

public class AndPredicate<DocumentVisitorType> implements DocumentQueryPredicate<DocumentVisitorType> {
    private final List<DocumentQueryPredicate<DocumentVisitorType>> andPredicates;

    public AndPredicate(final List<DocumentQueryPredicate<DocumentVisitorType>> andPredicates) {
        this.andPredicates = andPredicates;
    }

    public List<DocumentQueryPredicate<DocumentVisitorType>> getAndPredicates() {
        return this.andPredicates;
    }

    @Override
    public void accept(final QueryPredicateVisitor<DocumentVisitorType> visitor) {
        visitor.visit(this);
    }
}
