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
     * @return document's attribute described in {@link DocumentPathExpression} or {@code null} if the defined path does
     *         not exist in the given document
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
        final DocumentNode<VisitorType> nextNode = traverseStepper.apply(thisNode,
                this.pathExpression.getSubPath(0, position + 1));
        if (nextNode == null) {
            return null;
        }
        return performStep(nextNode, position + 1);
    }

    @java.lang.SuppressWarnings("squid:S119") // VisitorType does not fit naming conventions.
    private BiFunction<DocumentNode<VisitorType>, DocumentPathExpression, DocumentNode<VisitorType>> getStepperFor(
            final PathSegment pathSegment) {
        final WalkVisitor<VisitorType> visitor = new WalkVisitor<>();
        pathSegment.accept(visitor);
        return visitor.getStepper();
    }

    @java.lang.SuppressWarnings("squid:S119") // VisitorType does not fit naming conventions.
    private class WalkVisitor<VisitorType> implements PathSegmentVisitor {
        BiFunction<DocumentNode<VisitorType>, DocumentPathExpression, DocumentNode<VisitorType>> stepper;

        @Override
        public void visit(final ObjectLookupPathSegment objectLookupPathSegment) {
            this.stepper = (thisNode, pathToThisNode) -> {
                final String key = objectLookupPathSegment.getLookupKey();
                if (!(thisNode instanceof DocumentObject)) {
                    return null;
                }
                final DocumentObject<VisitorType> thisObject = (DocumentObject<VisitorType>) thisNode;
                if (!thisObject.hasKey(key)) {
                    return null;
                }
                return thisObject.get(key);
            };
        }

        @Override
        public void visit(final ArrayLookupPathSegment arrayLookupPathSegment) {
            this.stepper = (thisNode, pathToThisNode) -> {
                if (!(thisNode instanceof DocumentArray)) {
                    return null;
                }
                final DocumentArray<VisitorType> thisArray = (DocumentArray<VisitorType>) thisNode;
                if (thisArray.size() < arrayLookupPathSegment.getLookupIndex()) {
                    return null;
                }
                return thisArray.getValue(arrayLookupPathSegment.getLookupIndex());
            };
        }

        @Override
        public void visit(final ArrayAllPathSegment arrayAllPathSegment) {
            this.stepper = (thisNode, pathToThisNode) -> {
                if (!(thisNode instanceof DocumentArray)) {
                    return null;
                }
                final DocumentArray<VisitorType> thisArray = (DocumentArray<VisitorType>) thisNode;
                final int iterationIndex = DocumentPathWalker.this.iterationStateProvider.getIndexFor(pathToThisNode);
                return thisArray.getValue(iterationIndex);
            };
        }

        public BiFunction<DocumentNode<VisitorType>, DocumentPathExpression, DocumentNode<VisitorType>> getStepper() {
            return this.stepper;
        }
    }
}
