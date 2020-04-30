package com.exasol.adapter.dynamodb.documentpath;

/**
 * This is an iterator that does exactly one iterations. It is used if no iteration needs to be done.
 */
public class StaticDocumentPathIterator implements DocumentPathIterator {
    private boolean wasCalled = false;

    @Override
    public boolean next() {
        if (!this.wasCalled) {
            this.wasCalled = true;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public int getIndexFor(final DocumentPathExpression pathToArrayAll) {
        throw new IllegalStateException("The requested path is longer than the unwinded one.");
    }
}
