package com.exasol.adapter.dynamodb.remotetablequery.normalizer;

import org.logicng.formulas.Formula;
import org.logicng.transformations.dnf.DNFFactorization;

import com.exasol.adapter.dynamodb.remotetablequery.QueryPredicate;

/**
 * This class normalizes a {@link QueryPredicate} structure to the disjunctive normal form.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public class DnfNormalizer<DocumentVisitorType> {
    private final QueryPredicateToLogicngConverter<DocumentVisitorType> converter;

    /**
     * Create an instance of {@link DnfNormalizer}.
     */
    public DnfNormalizer() {
        this.converter = new QueryPredicateToLogicngConverter<>();
    }

    /**
     * Normalizes a predicate to the disjunctive normal form.
     * 
     * @param predicate predicate structure to normalize
     * @return normalized predicate structure
     */
    public DnfOr<DocumentVisitorType> normalize(final QueryPredicate<DocumentVisitorType> predicate) {
        final QueryPredicateToLogicngConverter.Result<DocumentVisitorType> conversionResult = this.converter
                .convert(predicate);
        final Formula dnfFormula = conversionResult.getLogicngFormula().transform(new DNFFactorization());
        final QueryPredicate<DocumentVisitorType> dnf = new LogicngToQueryPredicateConverter<>(
                conversionResult.getVariablesMapping()).convert(dnfFormula);
        return new DnfClassStructureFactory<DocumentVisitorType>().build(dnf);
    }
}
