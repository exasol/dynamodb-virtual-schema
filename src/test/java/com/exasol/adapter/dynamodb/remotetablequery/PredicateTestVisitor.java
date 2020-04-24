package com.exasol.adapter.dynamodb.remotetablequery;

class PredicateTestVisitor implements QueryPredicateVisitor<Object> {
    private Visited visited;

    @Override
    public void visit(final ColumnLiteralComparisonPredicate<Object> columnLiteralComparisonPredicate) {
        this.visited = Visited.COLUMN_LITERAL_COMPARISON;
    }

    @Override
    public void visit(final BinaryLogicalOperator<Object> binaryLogicalOperator) {
        this.visited = Visited.BINARY_LOGICAL_OPERATOR;
    }

    @Override
    public void visit(final NoPredicate<Object> noPredicate) {
        this.visited = Visited.NO;
    }

    public Visited getVisited() {
        return this.visited;
    }

    enum Visited {
        BINARY_LOGICAL_OPERATOR, OR, NO, COLUMN_LITERAL_COMPARISON
    }
}
