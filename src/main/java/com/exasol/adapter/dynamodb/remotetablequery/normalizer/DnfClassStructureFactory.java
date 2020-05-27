package com.exasol.adapter.dynamodb.remotetablequery.normalizer;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.exasol.adapter.dynamodb.remotetablequery.*;

/**
 * This factory builds {@link DnfOr} class structures from {@link QueryPredicate} structures.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
class DnfClassStructureFactory<DocumentVisitorType> {

    /**
     * Build {@link DnfOr} class structures from {@link QueryPredicate} structures.
     * 
     * @param predicate dnf normalized {@link QueryPredicate} structure
     * @return {@link DnfOr} class structures
     */
    DnfOr<DocumentVisitorType> build(final QueryPredicate<DocumentVisitorType> predicate) {
        final FirstLevelVisitor<DocumentVisitorType> visitor = new FirstLevelVisitor<>();
        predicate.accept(visitor);
        return visitor.result;
    }

    private static class FirstLevelVisitor<DocumentVisitorType> implements QueryPredicateVisitor<DocumentVisitorType> {
        private DnfOr<DocumentVisitorType> result;

        @Override
        public void visit(final ComparisonPredicate<DocumentVisitorType> comparisonPredicate) {
            this.result = new DnfOr<>(List.of(visitSecondLevel(comparisonPredicate)));
        }

        @Override
        public void visit(final LogicalOperator<DocumentVisitorType> logicalOperator) {
            if (logicalOperator.getOperator().equals(LogicalOperator.Operator.OR)) {
                final List<DnfAnd<DocumentVisitorType>> operands = logicalOperator.getOperands().stream()
                        .map(this::visitSecondLevel).collect(Collectors.toList());
                this.result = new DnfOr<>(operands);
            } else {
                this.result = new DnfOr<>(List.of(visitSecondLevel(logicalOperator)));
            }
        }

        @Override
        public void visit(final NoPredicate<DocumentVisitorType> noPredicate) {
            this.result = new DnfOr<>(Collections.emptyList());
        }

        @Override
        public void visit(final NotPredicate<DocumentVisitorType> notPredicate) {
            this.result = new DnfOr<>(List.of(visitSecondLevel(notPredicate)));
        }

        private DnfAnd<DocumentVisitorType> visitSecondLevel(final QueryPredicate<DocumentVisitorType> predicate) {
            final SecondLevelVisitor<DocumentVisitorType> visitor = new SecondLevelVisitor<>();
            predicate.accept(visitor);
            return visitor.result;
        }
    }

    private static class SecondLevelVisitor<DocumentVisitorType> implements QueryPredicateVisitor<DocumentVisitorType> {
        private DnfAnd<DocumentVisitorType> result;

        @Override
        public void visit(final ComparisonPredicate<DocumentVisitorType> comparisonPredicate) {
            this.result = new DnfAnd<>(List.of(visitThirdLevel(comparisonPredicate)));
        }

        @Override
        public void visit(final LogicalOperator<DocumentVisitorType> logicalOperator) {
            if (logicalOperator.getOperator().equals(LogicalOperator.Operator.AND)) {
                final List<DnfComparison<DocumentVisitorType>> operands = logicalOperator.getOperands().stream()
                        .map(this::visitThirdLevel).collect(Collectors.toList());
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
            this.result = new DnfAnd<>(List.of(visitThirdLevel(notPredicate)));
        }

        private DnfComparison<DocumentVisitorType> visitThirdLevel(
                final QueryPredicate<DocumentVisitorType> predicate) {
            final ThirdLevelVisitor<DocumentVisitorType> visitor = new ThirdLevelVisitor<>(false);
            predicate.accept(visitor);
            return visitor.result;
        }
    }

    private static class ThirdLevelVisitor<DocumentVisitorType> implements QueryPredicateVisitor<DocumentVisitorType> {
        private final boolean isNegated;
        private DnfComparison<DocumentVisitorType> result;

        private ThirdLevelVisitor(final boolean isNegated) {
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
            final ThirdLevelVisitor<DocumentVisitorType> visitor = new ThirdLevelVisitor<>(!this.isNegated);
            notPredicate.getPredicate().accept(visitor);
            this.result = visitor.result;
        }
    }
}
