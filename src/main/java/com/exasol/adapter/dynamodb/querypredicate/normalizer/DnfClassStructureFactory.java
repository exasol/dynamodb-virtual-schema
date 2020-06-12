package com.exasol.adapter.dynamodb.querypredicate.normalizer;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import com.exasol.adapter.dynamodb.querypredicate.*;

/**
 * This factory builds {@link DnfOr} class structures from {@link QueryPredicate} structures. The input predicate
 * structure must fulfil the DNF. A valid consists of disjunctions that contain conjunctions that then contain variables
 * or negated variables.
 *
 * Valid DNFs are for example: {@code A and B}, {@code A or B}, {@code A or (B and C)}, {@code A or (B and !C)}
 * 
 * Examples for invalid DNFs: {@code A and (B or C)}, {@code !(A or B)}
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
class DnfClassStructureFactory<DocumentVisitorType> {

    /**
     * Build {@link DnfOr} class structures from a DNF normalized {@link QueryPredicate} structures.
     * 
     * @param predicate DNF normalized {@link QueryPredicate} structure
     * @return {@link DnfOr} class structures
     */
    DnfOr<DocumentVisitorType> build(final QueryPredicate<DocumentVisitorType> predicate) {
        final DisjunctionExtractor<DocumentVisitorType> visitor = new DisjunctionExtractor<>();
        predicate.accept(visitor);
        return visitor.result;
    }

    private static class DisjunctionExtractor<DocumentVisitorType>
            implements QueryPredicateVisitor<DocumentVisitorType> {
        private DnfOr<DocumentVisitorType> result;

        @Override
        public void visit(final ComparisonPredicate<DocumentVisitorType> comparisonPredicate) {
            this.result = new DnfOr<>(Set.of(visitSecondLevel(comparisonPredicate)));
        }

        @Override
        public void visit(final LogicalOperator<DocumentVisitorType> logicalOperator) {
            if (logicalOperator.getOperator().equals(LogicalOperator.Operator.OR)) {
                final Set<DnfAnd<DocumentVisitorType>> operands = logicalOperator.getOperands().stream()
                        .map(this::visitSecondLevel).collect(Collectors.toSet());
                this.result = new DnfOr<>(operands);
            } else {
                this.result = new DnfOr<>(Set.of(visitSecondLevel(logicalOperator)));
            }
        }

        @Override
        public void visit(final NoPredicate<DocumentVisitorType> noPredicate) {
            this.result = new DnfOr<>(Collections.emptySet());
        }

        @Override
        public void visit(final NotPredicate<DocumentVisitorType> notPredicate) {
            this.result = new DnfOr<>(Set.of(visitSecondLevel(notPredicate)));
        }

        private DnfAnd<DocumentVisitorType> visitSecondLevel(final QueryPredicate<DocumentVisitorType> predicate) {
            final ConjunctionExtractor<DocumentVisitorType> visitor = new ConjunctionExtractor<>();
            predicate.accept(visitor);
            return visitor.result;
        }
    }

    private static class ConjunctionExtractor<DocumentVisitorType>
            implements QueryPredicateVisitor<DocumentVisitorType> {
        private DnfAnd<DocumentVisitorType> result;

        @Override
        public void visit(final ComparisonPredicate<DocumentVisitorType> comparisonPredicate) {
            this.result = new DnfAnd<>(Set.of(visitThirdLevel(comparisonPredicate)));
        }

        @Override
        public void visit(final LogicalOperator<DocumentVisitorType> logicalOperator) {
            if (logicalOperator.getOperator().equals(LogicalOperator.Operator.AND)) {
                final Set<DnfComparison<DocumentVisitorType>> operands = logicalOperator.getOperands().stream()
                        .map(this::visitThirdLevel).collect(Collectors.toSet());
                this.result = new DnfAnd<>(operands);
            } else {
                throw new IllegalArgumentException("Invalid DNF. ORs are not allowed at the second level.");
            }
        }

        @Override
        public void visit(final NoPredicate<DocumentVisitorType> noPredicate) {
            throw new IllegalArgumentException("Invalid DNF. NoPredicate are only allowed at the first level.");
        }

        @Override
        public void visit(final NotPredicate<DocumentVisitorType> notPredicate) {
            this.result = new DnfAnd<>(Set.of(visitThirdLevel(notPredicate)));
        }

        private DnfComparison<DocumentVisitorType> visitThirdLevel(
                final QueryPredicate<DocumentVisitorType> predicate) {
            final VariableExtractor<DocumentVisitorType> visitor = new VariableExtractor<>(false);
            predicate.accept(visitor);
            return visitor.result;
        }
    }

    private static class VariableExtractor<DocumentVisitorType> implements QueryPredicateVisitor<DocumentVisitorType> {
        private final boolean isNegated;
        private DnfComparison<DocumentVisitorType> result;

        private VariableExtractor(final boolean isNegated) {
            this.isNegated = isNegated;
        }

        @Override
        public void visit(final ComparisonPredicate<DocumentVisitorType> comparisonPredicate) {
            this.result = new DnfComparison<>(this.isNegated, comparisonPredicate);
        }

        @Override
        public void visit(final LogicalOperator<DocumentVisitorType> logicalOperator) {
            throw new IllegalArgumentException("Invalid DNF. ANDs and ORs are not allowed at the third level.");
        }

        @Override
        public void visit(final NoPredicate<DocumentVisitorType> noPredicate) {
            throw new IllegalArgumentException("Invalid DNF. NoPredicate are only allowed at the first level.");
        }

        @Override
        public void visit(final NotPredicate<DocumentVisitorType> notPredicate) {
            final VariableExtractor<DocumentVisitorType> visitor = new VariableExtractor<>(!this.isNegated);
            notPredicate.getPredicate().accept(visitor);
            this.result = visitor.result;
        }
    }
}
