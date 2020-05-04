package com.exasol.adapter.dynamodb.documentpath;

/**
 * This class represents the current iteration state of a {@link LoopDocumentPathIterator}.
 */
public class LoopDocumentPathIteratorState implements PathIterationStateProvider {
    private final DocumentPathExpression pathOfThisIterator;
    private final int currentIndex;
    private final PathIterationStateProvider nextState;

    LoopDocumentPathIteratorState(final DocumentPathExpression pathOfThisIterator, final int currentIndex,
            final PathIterationStateProvider nextState) {
        this.pathOfThisIterator = pathOfThisIterator;
        this.currentIndex = currentIndex;
        this.nextState = nextState;
    }

    @Override
    public int getIndexFor(final DocumentPathExpression pathToRequestedArrayAll) {
        if (pathToRequestedArrayAll.equals(this.pathOfThisIterator)) {// This request is
                                                                      // for our array
            return this.currentIndex;
        } else if (this.nextState != null && pathToRequestedArrayAll.startsWith(this.pathOfThisIterator)) {
            final DocumentPathExpression remainingPathToRequestedArrayAll = pathToRequestedArrayAll
                    .getSubPath(this.pathOfThisIterator.size(), pathToRequestedArrayAll.size());
            return this.nextState.getIndexFor(remainingPathToRequestedArrayAll);
        } else {
            throw new IllegalStateException("The requested path does not match the path that this iterator unwinds.");
        }
    }
}
