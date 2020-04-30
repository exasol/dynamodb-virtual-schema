package com.exasol.adapter.dynamodb.documentpath;

/**
 * Implementations of this interface iterates all combinations for the {@link ArrayAllPathSegment} in a given document.
 */
@java.lang.SuppressWarnings("squid:S119") // VisitorType does not fit naming conventions.
public interface DocumentPathIterator extends PathIterationStateProvider {
    /**
     * Moves iterator to the next combination.
     *
     * @return {@code true} if could move to next; {@code false} if there was no remaining combination to iterate.
     */
    public boolean next();
}
