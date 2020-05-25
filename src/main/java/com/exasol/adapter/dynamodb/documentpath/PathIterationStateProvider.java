package com.exasol.adapter.dynamodb.documentpath;

/**
 * This interface defines the access on the current state of a path iteration (defined by a
 * {@link DocumentPathIterator}). As multiple {@link ArrayAllPathSegment} can get iterated at a time, this state is more
 * complex than just a number. Instead for each {@link ArrayAllPathSegment} that is iterated over the current index can
 * be queried individually using {@link #getIndexFor(DocumentPathExpression)}.
 */
public interface PathIterationStateProvider {

    /**
     * Get the array index for the {@link ArrayAllPathSegment} defined in pathToArrayAll, to that the iterator points at
     * the moment.
     * 
     * @param pathToArrayAll path to the {@link ArrayAllPathSegment} for which the iteration index is requested
     * @return iteration index
     */
    public int getIndexFor(DocumentPathExpression pathToArrayAll);
}
