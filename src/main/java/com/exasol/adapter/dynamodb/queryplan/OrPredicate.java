package com.exasol.adapter.dynamodb.queryplan;

import java.util.List;

public class OrPredicate<DocumentVisitorType> implements DocumentQueryPredicate<DocumentVisitorType> {
    private final List<DocumentQueryPredicate<DocumentVisitorType>> orPredicates;

    public OrPredicate(final List<DocumentQueryPredicate<DocumentVisitorType>> orPredicates) {
        this.orPredicates = orPredicates;
    }

    public List<DocumentQueryPredicate<DocumentVisitorType>> getOrPredicates() {
        return this.orPredicates;
    }

    @Override
    public void accept(final QueryPredicateVisitor<DocumentVisitorType> visitor) {
        visitor.visit(this);
    }
}
