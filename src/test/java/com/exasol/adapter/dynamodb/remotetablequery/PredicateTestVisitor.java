package com.exasol.adapter.dynamodb.remotetablequery;

class PredicateTestVisitor implements QueryPredicateVisitor<Object> {
    private Visited visited;

    @Override
    public void visit(final ComparisonPredicate<Object> comparisonPredicate) {
        this.visited = Visited.COMPARISON;
    }

    @Override
    public void visit(final LogicalOperator<Object> logicalOperator) {
        this.visited = Visited.BINARY_LOGICAL_OPERATOR;
    }

    @Override
    public void visit(final NoPredicate<Object> noPredicate) {
        this.visited = Visited.NO;
    }

    @Override
    public void visit(final NotPredicate<Object> notPredicate) {
        this.visited = Visited.NOT;
    }

    public Visited getVisited() {
        return this.visited;
    }

    enum Visited {
        BINARY_LOGICAL_OPERATOR, NO, COMPARISON, NOT
    }
}
