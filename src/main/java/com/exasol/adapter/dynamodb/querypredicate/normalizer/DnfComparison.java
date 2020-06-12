package com.exasol.adapter.dynamodb.querypredicate.normalizer;

import java.util.Objects;

import com.exasol.adapter.dynamodb.querypredicate.ComparisonPredicate;
import com.exasol.adapter.dynamodb.querypredicate.NotPredicate;
import com.exasol.adapter.dynamodb.querypredicate.QueryPredicate;

/**
 * This class represents a comparison predicate in a disjunctive normal form (DNF).
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public final class DnfComparison<DocumentVisitorType> {
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

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof DnfComparison)) {
            return false;
        }
        final DnfComparison<?> that = (DnfComparison<?>) other;
        return this.isNegated == that.isNegated && this.comparisonPredicate.equals(that.comparisonPredicate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.isNegated, this.comparisonPredicate.hashCode());
    }
}
