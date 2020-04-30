package com.exasol.adapter.dynamodb.documentpath;

import com.exasol.adapter.dynamodb.documentnode.DocumentArray;
import com.exasol.adapter.dynamodb.documentnode.DocumentNode;

/**
 * This class iterates over {@link ArrayAllPathSegment}.
 * 
 * @param <VisitorType>
 */
@java.lang.SuppressWarnings("squid:S119") // VisitorType does not fit naming conventions.
public class LoopDocumentPathIterator<VisitorType> implements DocumentPathIterator {
    private final int arraySize;
    private final DocumentPathExpression pathOfThisIterator;
    private final DocumentPathExpression pathOfNextIterator;
    private final DocumentNode<VisitorType> document;
    private int currentIndex = -1;
    private DocumentPathIterator nextIterator;

    /**
     * Creates an instance of {@link LoopDocumentPathIterator}.
     * 
     * @param path     path definition
     * @param document document to iterate
     */
    public LoopDocumentPathIterator(final DocumentPathExpression path, final DocumentNode<VisitorType> document) {
        this.document = document;
        final int indexOfFirstArrayAllSegment = path.indexOfFirstArrayAllSegment();
        this.pathOfThisIterator = path.getSubPath(0, indexOfFirstArrayAllSegment + 1);
        this.pathOfNextIterator = path.getSubPath(indexOfFirstArrayAllSegment + 1, path.size());
        final DocumentPathExpression pathToThisArray = path.getSubPath(0, indexOfFirstArrayAllSegment);
        final DocumentArray<VisitorType> arrayToIterate = (DocumentArray<VisitorType>) new LinearDocumentPathWalker<VisitorType>(
                pathToThisArray).walkThroughDocument(document);
        this.arraySize = arrayToIterate.size();
    }

    /**
     * Moves iterator to the next combination.
     * 
     * @return {@code true} if could move to next; {@code false} if there was no remaining combination to iterate.
     */
    public boolean next() {
        while (true) {
            if (this.nextIterator != null && this.nextIterator.next()) {
                return true;
            } else if (hasSelfNext()) {
                loadNextIterator();
            } else {
                return false;
            }
        }
    }

    private void loadNextIterator() {
        this.currentIndex++;
        final DocumentNode<VisitorType> subDocument = new DocumentPathWalker<VisitorType>(this.pathOfThisIterator, this)
                .walkThroughDocument(this.document);
        this.nextIterator = new DocumentPathIteratorFactory<VisitorType>().buildFor(this.pathOfNextIterator,
                subDocument);
    }

    private boolean hasSelfNext() {
        return this.currentIndex + 1 < this.arraySize;
    }

    @Override
    public int getIndexFor(final DocumentPathExpression pathToRequestedArrayAll) {
        if (pathToRequestedArrayAll.equals(this.pathOfThisIterator)) {// This request is for our array
            return this.currentIndex;
        } else if (this.nextIterator != null && pathToRequestedArrayAll.startsWith(this.pathOfThisIterator)) {
            final DocumentPathExpression remainingPathToRequestedArrayAll = pathToRequestedArrayAll
                    .getSubPath(this.pathOfThisIterator.size(), pathToRequestedArrayAll.size());
            return this.nextIterator.getIndexFor(remainingPathToRequestedArrayAll);
        } else {
            throw new IllegalStateException("The requested path does not match the path that this iterator unwinds.");
        }
    }
}
