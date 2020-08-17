package com.exasol.adapter.document.querypredicate;

class PredicateTestVisitor implements QueryPredicateVisitor {
    private Visited visited;

    @Override
    public void visit(final ComparisonPredicate comparisonPredicate) {
        this.visited = Visited.COMPARISON;
    }

    @Override
    public void visit(final LogicalOperator logicalOperator) {
        this.visited = Visited.BINARY_LOGICAL_OPERATOR;
    }

    @Override
    public void visit(final NoPredicate noPredicate) {
        this.visited = Visited.NO;
    }

    @Override
    public void visit(final NotPredicate notPredicate) {
        this.visited = Visited.NOT;
    }

    public Visited getVisited() {
        return this.visited;
    }

    enum Visited {
        BINARY_LOGICAL_OPERATOR, NO, COMPARISON, NOT
    }
}
