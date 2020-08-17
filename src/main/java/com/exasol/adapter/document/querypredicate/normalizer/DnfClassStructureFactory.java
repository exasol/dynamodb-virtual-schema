package com.exasol.adapter.document.querypredicate.normalizer;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import com.exasol.adapter.document.querypredicate.*;

/**
 * This factory builds {@link DnfOr} class structures from {@link QueryPredicate} structures. The input predicate
 * structure must fulfill the requirements of a valid DNF. A valid DNF consists of disjunctions that contain conjunctions that then contain variables
 * or negated variables.
 *
 * Valid DNFs are for example: {@code A and B}, {@code A or B}, {@code A or (B and C)}, {@code A or (B and !C)}
 * 
 * Examples for invalid DNFs: {@code A and (B or C)}, {@code !(A or B)}
 */
class DnfClassStructureFactory {

    /**
     * Build {@link DnfOr} class structures from a DNF normalized {@link QueryPredicate} structures.
     * 
     * @param predicate DNF normalized {@link QueryPredicate} structure
     * @return {@link DnfOr} class structures
     */
    DnfOr build(final QueryPredicate predicate) {
        final DisjunctionExtractor visitor = new DisjunctionExtractor();
        predicate.accept(visitor);
        return visitor.result;
    }

    private static class DisjunctionExtractor implements QueryPredicateVisitor {
        private DnfOr result;

        @Override
        public void visit(final ComparisonPredicate comparisonPredicate) {
            this.result = new DnfOr(Set.of(visitSecondLevel(comparisonPredicate)));
        }

        @Override
        public void visit(final LogicalOperator logicalOperator) {
            if (logicalOperator.getOperator().equals(LogicalOperator.Operator.OR)) {
                final Set<DnfAnd> operands = logicalOperator.getOperands().stream()
                        .map(this::visitSecondLevel).collect(Collectors.toSet());
                this.result = new DnfOr(operands);
            } else {
                this.result = new DnfOr(Set.of(visitSecondLevel(logicalOperator)));
            }
        }

        @Override
        public void visit(final NoPredicate noPredicate) {
            this.result = new DnfOr(Collections.emptySet());
        }

        @Override
        public void visit(final NotPredicate notPredicate) {
            this.result = new DnfOr(Set.of(visitSecondLevel(notPredicate)));
        }

        private DnfAnd visitSecondLevel(final QueryPredicate predicate) {
            final ConjunctionExtractor visitor = new ConjunctionExtractor();
            predicate.accept(visitor);
            return visitor.result;
        }
    }

    private static class ConjunctionExtractor implements QueryPredicateVisitor {
        private DnfAnd result;

        @Override
        public void visit(final ComparisonPredicate comparisonPredicate) {
            this.result = new DnfAnd(Set.of(visitThirdLevel(comparisonPredicate)));
        }

        @Override
        public void visit(final LogicalOperator logicalOperator) {
            if (logicalOperator.getOperator().equals(LogicalOperator.Operator.AND)) {
                final Set<DnfComparison> operands = logicalOperator.getOperands().stream()
                        .map(this::visitThirdLevel).collect(Collectors.toSet());
                this.result = new DnfAnd(operands);
            } else {
                throw new IllegalArgumentException("Invalid DNF. ORs are not allowed at the second level.");
            }
        }

        @Override
        public void visit(final NoPredicate noPredicate) {
            throw new IllegalArgumentException("Invalid DNF. NoPredicate are only allowed at the first level.");
        }

        @Override
        public void visit(final NotPredicate notPredicate) {
            this.result = new DnfAnd(Set.of(visitThirdLevel(notPredicate)));
        }

        private DnfComparison visitThirdLevel(final QueryPredicate predicate) {
            final VariableExtractor visitor = new VariableExtractor(false);
            predicate.accept(visitor);
            return visitor.result;
        }
    }

    private static class VariableExtractor implements QueryPredicateVisitor {
        private final boolean isNegated;
        private DnfComparison result;

        private VariableExtractor(final boolean isNegated) {
            this.isNegated = isNegated;
        }

        @Override
        public void visit(final ComparisonPredicate comparisonPredicate) {
            this.result = new DnfComparison(this.isNegated, comparisonPredicate);
        }

        @Override
        public void visit(final LogicalOperator logicalOperator) {
            throw new IllegalArgumentException("Invalid DNF. ANDs and ORs are not allowed at the third level.");
        }

        @Override
        public void visit(final NoPredicate noPredicate) {
            throw new IllegalArgumentException("Invalid DNF. NoPredicate are only allowed at the first level.");
        }

        @Override
        public void visit(final NotPredicate notPredicate) {
            final VariableExtractor visitor = new VariableExtractor(!this.isNegated);
            notPredicate.getPredicate().accept(visitor);
            this.result = visitor.result;
        }
    }
}
