package com.exasol.adapter.document.querypredicate;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.exasol.adapter.document.mapping.ColumnMapping;

/**
 * This collects all columns that are involved in comparisons in a QueryPredicate structure.
 */
public class InvolvedColumnCollector {

    /**
     * Collects all columns that are involved in comparisons in a QueryPredicate structure.
     * 
     * @param predicate query predicate structure to collect the columns from
     * @return list of columns
     */
    public List<ColumnMapping> collectInvolvedColumns(final QueryPredicate predicate) {
        final Visitor visitor = new Visitor();
        predicate.accept(visitor);
        return visitor.getInvolvedColumns();
    }

    private static class Visitor implements QueryPredicateVisitor {
        private List<ColumnMapping> involvedColumns;

        @Override
        public void visit(final ComparisonPredicate comparisonPredicate) {
            this.involvedColumns = comparisonPredicate.getComparedColumns();
        }

        @Override
        public void visit(final LogicalOperator logicalOperator) {
            this.involvedColumns = logicalOperator.getOperands().stream()
                    .flatMap(predicate -> this.callRecursive(predicate).stream()).collect(Collectors.toList());
        }

        @Override
        public void visit(final NoPredicate noPredicate) {
            this.involvedColumns = Collections.emptyList();
        }

        @Override
        public void visit(final NotPredicate notPredicate) {
            this.involvedColumns = callRecursive(notPredicate.getPredicate());
        }

        public List<ColumnMapping> getInvolvedColumns() {
            return this.involvedColumns;
        }

        private List<ColumnMapping> callRecursive(final QueryPredicate predicate) {
            final Visitor visitor = new Visitor();
            predicate.accept(visitor);
            return visitor.getInvolvedColumns();
        }
    }
}
