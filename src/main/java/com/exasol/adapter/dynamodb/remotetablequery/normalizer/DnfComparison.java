package com.exasol.adapter.dynamodb.remotetablequery.normalizer;

import com.exasol.adapter.dynamodb.remotetablequery.ComparisonPredicate;
import com.exasol.adapter.dynamodb.remotetablequery.NotPredicate;
import com.exasol.adapter.dynamodb.remotetablequery.QueryPredicate;

/**
 * This class represents a comparison predicate in a disjunctive normal form (DNF).
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public class DnfComparison<DocumentVisitorType> {
    private final boolean isNegated;
    private final ComparisonPredicate<DocumentVisitorType> comparisonPredicate;

    /**
     * Create an instance of {@link DnfComparison}
     * 
     * @param isNegated           {@code true} if the predicate is negated
     * @param comparisonPredicate the comparison predicate
     */
    public DnfComparison(final boolean isNegated, final ComparisonPredicate<DocumentVisitorType> comparisonPredicate) {
        this.isNegated = isNegated;
        this.comparisonPredicate = comparisonPredicate;
    }

    /**
     * Get if this predicate is negated.
     * 
     * @return {@code true} if the predicate is negated
     */
    public boolean isNegated() {
        return this.isNegated;
    }

    /**
     * Get the comparison predicate.
     * 
     * @return comparison predicate
     */
    public ComparisonPredicate<DocumentVisitorType> getComparisonPredicate() {
        return this.comparisonPredicate;
    }

    public QueryPredicate<DocumentVisitorType> asQueryPredicate() {
        if (!this.isNegated) {
            return this.comparisonPredicate;
        } else {
            return new NotPredicate<>(this.comparisonPredicate);
        }
    }

    @Override
    public String toString() {
        if (this.isNegated) {
            return "NOT(" + this.comparisonPredicate.toString() + ")";
        } else {
            return this.comparisonPredicate.toString();
        }
    }
}
