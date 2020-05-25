package com.exasol.adapter.dynamodb.remotetablequery.normalizer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.logicng.formulas.Formula;
import org.logicng.formulas.FormulaFactory;
import org.logicng.formulas.Variable;

import com.exasol.adapter.dynamodb.remotetablequery.*;

/**
 * This class converts a {@link QueryPredicate} structure into a LogicNG {@link Formula}.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
class QueryPredicateToLogicngConverter<DocumentVisitorType> {

    /**
     * Converts a {@link QueryPredicate} structure into a LogicNG {@link Formula}.
     *
     * @param predicate {@link QueryPredicate} structure to convert
     * @return {@link Result} containing LogicNG {@link Formula} and variables mapping
     */
    public Result<DocumentVisitorType> convert(final QueryPredicate<DocumentVisitorType> predicate) {
        final VariablesMappingBuilder<DocumentVisitorType> variablesMappingBuilder = new VariablesMappingBuilder<>();
        final Visitor<DocumentVisitorType> visitor = new Visitor<>(variablesMappingBuilder);
        predicate.accept(visitor);
        return new Result<>(visitor.getFormula(), variablesMappingBuilder.getVariablesMapping());
    }

    private static class Visitor<DocumentVisitorType> implements QueryPredicateVisitor<DocumentVisitorType> {
        private final VariablesMappingBuilder<DocumentVisitorType> variablesMappingBuilder;
        private Formula formula;

        private Visitor(final VariablesMappingBuilder<DocumentVisitorType> variablesMappingBuilder) {

            this.variablesMappingBuilder = variablesMappingBuilder;
        }

        @Override
        public void visit(
                final ColumnLiteralComparisonPredicate<DocumentVisitorType> columnLiteralComparisonPredicate) {
            this.formula = this.variablesMappingBuilder.add(columnLiteralComparisonPredicate);
        }

        @Override
        public void visit(final LogicalOperator<DocumentVisitorType> logicalOperator) {
            final FormulaFactory formulaFactory = new FormulaFactory();
            final List<Formula> operandsFormulas = logicalOperator.getOperands().stream().map(this::callRecursive)
                    .collect(Collectors.toList());
            if (logicalOperator.getOperator() == LogicalOperator.Operator.AND) {
                this.formula = formulaFactory.and(operandsFormulas);
            } else {
                this.formula = formulaFactory.or(operandsFormulas);
            }
        }

        @Override
        public void visit(final NoPredicate<DocumentVisitorType> noPredicate) {
            this.formula = new FormulaFactory().constant(true);
        }

        private Formula callRecursive(final QueryPredicate<DocumentVisitorType> predicate) {
            final Visitor<DocumentVisitorType> visitor = new Visitor<>(this.variablesMappingBuilder);
            predicate.accept(visitor);
            return visitor.getFormula();
        }

        public Formula getFormula() {
            return this.formula;
        }
    }

    private static class VariablesMappingBuilder<DocumentVisitorType> {
        private final Map<Variable, QueryPredicate<DocumentVisitorType>> variablesMapping;
        private final Map<QueryPredicate<DocumentVisitorType>, Variable> inverseVariablesMapping;
        private final FormulaFactory formulaFactory;
        private int uniqueVariableNameCounter = 0;

        public VariablesMappingBuilder() {
            this.variablesMapping = new HashMap<>();
            this.inverseVariablesMapping = new HashMap<>();
            this.formulaFactory = new FormulaFactory();
        }

        public Variable add(final QueryPredicate<DocumentVisitorType> predicateToAdd) {
            if (this.inverseVariablesMapping.containsKey(predicateToAdd)) {
                return this.inverseVariablesMapping.get(predicateToAdd);
            }
            final String variableName = "Variable" + this.uniqueVariableNameCounter++;
            final Variable variable = this.formulaFactory.variable(variableName);
            this.variablesMapping.put(variable, predicateToAdd);
            this.inverseVariablesMapping.put(predicateToAdd, variable);
            return variable;
        }

        public Map<Variable, QueryPredicate<DocumentVisitorType>> getVariablesMapping() {
            return this.variablesMapping;
        }
    }

    /**
     * This class represents the result of {@link #convert(QueryPredicate)}.
     */
    public static class Result<DocumentVisitorType> {
        private final Formula logicngFormula;
        private final Map<Variable, QueryPredicate<DocumentVisitorType>> variablesMapping;

        private Result(final Formula logicngFormula,
                final Map<Variable, QueryPredicate<DocumentVisitorType>> variablesMapping) {
            this.logicngFormula = logicngFormula;
            this.variablesMapping = variablesMapping;
        }

        /**
         * Gives the formula in the representation of the LogicNG library.
         * 
         * @return LogicNg formula
         */
        public Formula getLogicngFormula() {
            return this.logicngFormula;
        }

        /**
         * Gives a map that maps the Variables in the formla to {@link QueryPredicate}s.
         * 
         * @return variable map
         */
        public Map<Variable, QueryPredicate<DocumentVisitorType>> getVariablesMapping() {
            return this.variablesMapping;
        }
    }
}
