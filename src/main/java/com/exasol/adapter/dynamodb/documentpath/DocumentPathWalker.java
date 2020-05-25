package com.exasol.adapter.dynamodb.documentpath;

import java.util.function.BiFunction;

import com.exasol.adapter.dynamodb.documentnode.DocumentArray;
import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentnode.DocumentObject;

/**
 * This class walks a given path defined in {@link DocumentPathExpression} through a {@link DocumentNode} structure.
 */
@java.lang.SuppressWarnings("squid:S119") // VisitorType does not fit naming conventions.
public class DocumentPathWalker<VisitorType> {
    private final DocumentPathExpression pathExpression;
    private final PathIterationStateProvider iterationStateProvider;

    /**
     * Create an instance of {@link DocumentPathWalker}.
     * 
     * @param pathExpression path to walk
     */
    public DocumentPathWalker(final DocumentPathExpression pathExpression,
            final PathIterationStateProvider iterationStateProvider) {
        this.pathExpression = pathExpression;
        this.iterationStateProvider = iterationStateProvider;
    }

    /**
     * Walks the path defined in constructor through the given document.
     * 
     * @param rootNode document to walk through
     * @return document's attribute described in {@link DocumentPathExpression}.
     * @throws DocumentPathWalkerException if defined path does not exist in the given document
     */
    public DocumentNode<VisitorType> walkThroughDocument(final DocumentNode<VisitorType> rootNode) {
        return this.performStep(rootNode, 0);
    }

    private DocumentNode<VisitorType> performStep(final DocumentNode<VisitorType> thisNode, final int position) {
        if (this.pathExpression.size() <= position) {
            return thisNode;
        }
        final BiFunction<DocumentNode<VisitorType>, DocumentPathExpression, DocumentNode<VisitorType>> stepper = getStepperFor(
                this.pathExpression.getSegments().get(position));
        return runTraverseStepper(stepper, thisNode, position);
    }

    private DocumentNode<VisitorType> runTraverseStepper(
            final BiFunction<DocumentNode<VisitorType>, DocumentPathExpression, DocumentNode<VisitorType>> traverseStepper,
            final DocumentNode<VisitorType> thisNode, final int position) {
        try {
            final DocumentNode<VisitorType> nextNode = traverseStepper.apply(thisNode,
                    this.pathExpression.getSubPath(0, position + 1));
            return performStep(nextNode, position + 1);
        } catch (final InternalPathWalkException exception) {
            throw addCurrentPathToException(exception, position);
        }
    }

    private DocumentPathWalkerException addCurrentPathToException(final InternalPathWalkException exception,
            final int position) {
        final DocumentPathExpression currentPath = this.pathExpression.getSubPath(0, position);
        final String currentPathString = new DocumentPathToStringConverter().convertToString(currentPath);
        return new DocumentPathWalkerException(exception.getMessage(), currentPathString);
    }

    @java.lang.SuppressWarnings("squid:S119") // VisitorType does not fit naming conventions.
    private BiFunction<DocumentNode<VisitorType>, DocumentPathExpression, DocumentNode<VisitorType>> getStepperFor(
            final PathSegment pathSegment) {
        final WalkVisitor<VisitorType> visitor = new WalkVisitor<>();
        pathSegment.accept(visitor);
        return visitor.getStepper();
    }

    /**
     * This exception is caught in {@link #performStep(DocumentNode, int)} and converted into an
     * {@link DocumentPathWalkerException}. This step is done for appending the current path.
     */
    private static class InternalPathWalkException extends RuntimeException {
        private static final long serialVersionUID = -4653933935917135457L;

        public InternalPathWalkException(final String message) {
            super(message);
        }
    }

    @java.lang.SuppressWarnings("squid:S119") // VisitorType does not fit naming conventions.
    private class WalkVisitor<VisitorType> implements PathSegmentVisitor {
        BiFunction<DocumentNode<VisitorType>, DocumentPathExpression, DocumentNode<VisitorType>> stepper;

        @Override
        public void visit(final ObjectLookupPathSegment objectLookupPathSegment) {
            this.stepper = (thisNode, pathToThisNode) -> {
                final String key = objectLookupPathSegment.getLookupKey();
                if (!(thisNode instanceof DocumentObject)) {
                    throw new InternalPathWalkException(
                            "Can't perform key lookup on non object. (requested key= " + key + ")");
                }
                final DocumentObject<VisitorType> thisObject = (DocumentObject<VisitorType>) thisNode;
                if (!thisObject.hasKey(key)) {
                    throw new InternalPathWalkException(
                            "The requested lookup key (" + key + ") is not present in this object.");
                }
                return thisObject.get(key);
            };
        }

        @Override
        public void visit(final ArrayLookupPathSegment arrayLookupPathSegment) {
            this.stepper = (thisNode, pathToThisNode) -> {
                final DocumentArray<VisitorType> thisArray = castNodeToArray(thisNode);
                try {
                    return thisArray.getValue(arrayLookupPathSegment.getLookupIndex());
                } catch (final IndexOutOfBoundsException exception) {
                    throw new InternalPathWalkException("Can't perform array lookup: " + exception.getMessage());
                }
            };
        }

        private DocumentArray<VisitorType> castNodeToArray(final DocumentNode<VisitorType> thisNode) {
            if (!(thisNode instanceof DocumentArray)) {
                throw new InternalPathWalkException("Can't perform array lookup on non array.");
            }
            return (DocumentArray<VisitorType>) thisNode;
        }

        @Override
        public void visit(final ArrayAllPathSegment arrayAllPathSegment) {
            this.stepper = (thisNode, pathToThisNode) -> {
                final DocumentArray<VisitorType> thisArray = castNodeToArray(thisNode);
                final int iterationIndex = DocumentPathWalker.this.iterationStateProvider.getIndexFor(pathToThisNode);
                return thisArray.getValue(iterationIndex);
            };
        }

        public BiFunction<DocumentNode<VisitorType>, DocumentPathExpression, DocumentNode<VisitorType>> getStepper() {
            return this.stepper;
        }
    }
}
