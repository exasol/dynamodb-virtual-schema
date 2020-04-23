package com.exasol.adapter.dynamodb.queryplan;

class PredicateTestVisitor implements QueryPredicateVisitor<Object> {
    private Visited visited;

    @Override
    public void visit(final ColumnLiteralComparisonPredicate<Object> columnLiteralComparisonPredicate) {
        this.visited = Visited.COLUMN_LITERAL_COMPARISON;
    }

    @Override
    public void visit(final AndPredicate<Object> andPredicate) {
        this.visited = Visited.AND;
    }

    @Override
    public void visit(final OrPredicate<Object> orPredicate) {
        this.visited = Visited.OR;
    }

    @Override
    public void visit(final NoPredicate<Object> noPredicate) {
        this.visited = Visited.NO;
    }

    public Visited getVisited() {
        return this.visited;
    }

    enum Visited {
        AND, OR, NO, COLUMN_LITERAL_COMPARISON
    }
}
