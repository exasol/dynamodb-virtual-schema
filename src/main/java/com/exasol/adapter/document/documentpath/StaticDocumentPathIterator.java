package com.exasol.adapter.document.documentpath;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * This is an iterator that does exactly one iteration. It is used if no iteration needs to be done.
 */
public class StaticDocumentPathIterator implements Iterator<PathIterationStateProvider>, PathIterationStateProvider {
    private boolean called = false;

    @Override
    public boolean hasNext() {
        return !this.called;
    }

    @Override
    public PathIterationStateProvider next() {
        if (hasNext()) {
            this.called = true;
            return this;
        } else {
            throw new NoSuchElementException("The are no more combinations to iterate.");
        }
    }

    @Override
    public int getIndexFor(final DocumentPathExpression pathToArrayAll) {
        throw new IllegalStateException("The requested path is longer than the unwinded one.");
    }
}
