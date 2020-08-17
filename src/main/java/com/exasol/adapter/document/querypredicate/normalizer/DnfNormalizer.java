package com.exasol.adapter.document.querypredicate.normalizer;

import org.logicng.formulas.Formula;
import org.logicng.transformations.dnf.DNFFactorization;

import com.exasol.adapter.document.querypredicate.QueryPredicate;

/**
 * This class normalizes a {@link QueryPredicate} structure to the disjunctive normal form.
 */
public class DnfNormalizer {
    private final QueryPredicateToLogicngConverter converter;

    /**
     * Create an instance of {@link DnfNormalizer}.
     */
    public DnfNormalizer() {
        this.converter = new QueryPredicateToLogicngConverter();
    }

    /**
     * Normalizes a predicate to the disjunctive normal form.
     * 
     * @param predicate predicate structure to normalize
     * @return normalized predicate structure
     */
    public DnfOr normalize(final QueryPredicate predicate) {
        final QueryPredicateToLogicngConverter.Result conversionResult = this.converter
                .convert(predicate);
        final Formula dnfFormula = conversionResult.getLogicngFormula().transform(new DNFFactorization());
        final QueryPredicate dnf = new LogicngToQueryPredicateConverter(
                conversionResult.getVariablesMapping()).convert(dnfFormula);
        return new DnfClassStructureFactory().build(dnf);
    }
}
