package com.exasol.adapter.document.querypredicate;

import static com.exasol.sql.expression.BooleanTerm.not;
import static com.exasol.sql.expression.ExpressionTerm.column;

import com.exasol.adapter.document.literalconverter.SqlLiteralToValueExpressionConverter;
import com.exasol.sql.expression.*;

/**
 * This class converts a {@link QueryPredicate} class structure to a {@link BooleanExpression} for the
 * sql-statement-builder
 */
public class QueryPredicateToBooleanExpressionConverter {

    public BooleanExpression convert(final QueryPredicate queryPredicate) {
        final Visitor visitor = new Visitor();
        queryPredicate.accept(visitor);
        return visitor.getResult();
    }

    private static class Visitor implements QueryPredicateVisitor, ComparisonPredicateVisitor {
        private BooleanExpression result;

        @Override
        public void visit(final ComparisonPredicate comparisonPredicate) {
            comparisonPredicate.accept((ComparisonPredicateVisitor) this);
        }

        @Override
        public void visit(final LogicalOperator logicalOperator) {
            final BooleanExpression[] convertedOperands = logicalOperator.getOperands().stream()
                    .map(this::callRecursive).toArray(BooleanExpression[]::new);
            if (logicalOperator.getOperator().equals(LogicalOperator.Operator.AND)) {
                this.result = new And(convertedOperands);
            } else {// OR
                this.result = new Or(convertedOperands);
            }
        }

        @Override
        public void visit(final NoPredicate noPredicate) {
            this.result = BooleanLiteral.of(true);
        }

        @Override
        public void visit(final NotPredicate notPredicate) {
            this.result = not(callRecursive(notPredicate.getPredicate()));
        }

        @Override
        public void visit(final ColumnLiteralComparisonPredicate columnLiteralComparisonPredicate) {
            final ValueExpression literal = SqlLiteralToValueExpressionConverter.getInstance()
                    .convert(columnLiteralComparisonPredicate.getLiteral());
            this.result = new Comparison(ComparisonOperator.EQUAL,
                    column(columnLiteralComparisonPredicate.getColumn().getExasolColumnName()), literal);
        }

        private BooleanExpression callRecursive(final QueryPredicate predicate) {
            final Visitor visitor = new Visitor();
            predicate.accept(visitor);
            return visitor.result;
        }

        public BooleanExpression getResult() {
            return this.result;
        }
    }
}
