package com.exasol.adapter.dynamodb.documentpath;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import com.exasol.adapter.dynamodb.documentnode.DocumentArray;
import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentnode.DocumentObject;

/**
 * This class walks a given path defined in {@link DocumentPathExpression} through a {@link DocumentNode} structure.
 */
@java.lang.SuppressWarnings("squid:S119") // VisitorType does not fit naming conventions.
public class DocumentPathWalker<VisitorType> {
    private final DocumentPathExpression pathExpression;

    /**
     * Creates an instance of {@link DocumentPathWalker}.
     * 
     * @param pathExpression path to walk
     */
    public DocumentPathWalker(final DocumentPathExpression pathExpression) {
        this.pathExpression = pathExpression;
    }

    /**
     * Walks the path defined in constructor through the given document.
     * 
     * @param rootNode document to walk through
     * @return documents attributes described in {@link DocumentPathExpression}.
     * @throws DocumentPathWalkerException if defined path does not exist in the given document
     */
    public List<DocumentNode<VisitorType>> walkThroughDocument(final DocumentNode<VisitorType> rootNode)
            throws DocumentPathWalkerException {
        return this.performStep(rootNode, 0);
    }

    private List<DocumentNode<VisitorType>> performStep(final DocumentNode<VisitorType> thisNode, final int position)
            throws DocumentPathWalkerException {
        if (this.pathExpression.size() <= position) {
            return List.of(thisNode);
        }
        final Function<DocumentNode<VisitorType>, List<DocumentNode<VisitorType>>> stepper = getStepperFor(
                this.pathExpression.getPath().get(position));
        return runTraverseStepper(stepper, thisNode, position);
    }

    private List<DocumentNode<VisitorType>> runTraverseStepper(
            final Function<DocumentNode<VisitorType>, List<DocumentNode<VisitorType>>> traverseStepper,
            final DocumentNode<VisitorType> thisNode, final int position) throws DocumentPathWalkerException {
        try {
            final ArrayList<DocumentNode<VisitorType>> results = new ArrayList<>();
            for (final DocumentNode<VisitorType> nextNode : traverseStepper.apply(thisNode)) {
                results.addAll(performStep(nextNode, position + 1));
            }
            return results;
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
    private Function<DocumentNode<VisitorType>, List<DocumentNode<VisitorType>>> getStepperFor(
            final PathSegment pathSegment) {
        final WalkVisitor<VisitorType> visitor = new WalkVisitor<>();
        pathSegment.accept(visitor);
        return visitor.getStepper();
    }

    @java.lang.SuppressWarnings("squid:S119") // VisitorType does not fit naming conventions.
    private static class WalkVisitor<VisitorType> implements PathSegmentVisitor {
        Function<DocumentNode<VisitorType>, List<DocumentNode<VisitorType>>> stepper;

        @Override
        public void visit(final ObjectLookupPathSegment objectLookupPathSegment) {
            this.stepper = thisNode -> {
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
                return List.of(thisObject.get(key));
            };
        }

        @Override
        public void visit(final ArrayLookupPathSegment arrayLookupPathSegment) {
            this.stepper = thisNode -> {
                final DocumentArray<VisitorType> thisArray = castNodeToArray(thisNode);
                try {
                    return List.of(thisArray.getValue(arrayLookupPathSegment.getLookupIndex()));
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
            this.stepper = thisNode -> {
                final DocumentArray<VisitorType> thisArray = castNodeToArray(thisNode);
                return thisArray.getValuesList();
            };
        }

        public Function<DocumentNode<VisitorType>, List<DocumentNode<VisitorType>>> getStepper() {
            return this.stepper;
        }
    }

    /**
     * This exception is caught in {@link #performStep(DocumentNode, int)} and converted into an
     * {@link DocumentPathWalkerException}. This step is done for appending the current path.
     */
    private static class InternalPathWalkException extends RuntimeException {
        public InternalPathWalkException(final String message) {
            super(message);
        }
    }
}
