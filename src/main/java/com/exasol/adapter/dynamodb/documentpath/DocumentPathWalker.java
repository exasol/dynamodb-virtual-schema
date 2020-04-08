package com.exasol.adapter.dynamodb.documentpath;

import java.util.List;
import java.util.function.Function;

import com.exasol.adapter.dynamodb.documentnode.DocumentArray;
import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentnode.DocumentObject;

/**
 * This class walks a given path defined in {@link DocumentPathExpression} through a {@link DocumentNode} structure.
 */
public class DocumentPathWalker {
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
     * @return documents attribute described in {@link DocumentPathExpression}
     * @throws DocumentPathWalkerException if defined path does not exist in the given document
     */
    public DocumentNode walk(final DocumentNode rootNode) throws DocumentPathWalkerException {
        return this.walk(rootNode, 0);
    }

    private DocumentNode walk(final DocumentNode thisNode, final int position) throws DocumentPathWalkerException {
        if (this.pathExpression.size() <= position) {
            return thisNode;
        }
        final DocumentPathExpression currentPath = this.pathExpression.getSubPath(0, position);
        final String currentPathString = new DocumentPathToStringConverter().convertToString(currentPath);
        final Function<DocumentNode, DocumentNode> converter = getConverterFor(
                this.pathExpression.getPath().get(position));
        try {
            final DocumentNode nextNode = converter.apply(thisNode);
            return walk(nextNode, position + 1);
        } catch (final NotAnObjectException exception) {
            throw new DocumentPathWalkerException(
                    "Can't perform key lookup on non object. (requested key= " + exception.lookupKey + ")",
                    currentPathString);
        } catch (final UnknownLookupKeyException exception) {
            throw new DocumentPathWalkerException(
                    "The requested lookup key (" + exception.lookupKey + ") is not present in this object.",
                    currentPathString);
        } catch (final NotAnArrayException exception){
            throw new DocumentPathWalkerException("Can't perform array lookup on non array.", currentPathString);
        }catch (final IndexOutOfBoundsException exception){
            throw new DocumentPathWalkerException("Can't perform array lookup: " + exception.getMessage(), currentPathString);
        }
    }

    private Function<DocumentNode, DocumentNode> getConverterFor(final PathSegment pathSegment) {
        final WalkVisitor visitor = new WalkVisitor();
        pathSegment.accept(visitor);
        return visitor.converter;
    }

    private static class WalkVisitor implements PathSegmentVisitor {
        Function<DocumentNode, DocumentNode> converter;

        @Override
        public void visit(final ObjectLookupPathSegment objectLookupPathSegment) {
            this.converter = thisNode -> {
                final String key = objectLookupPathSegment.getLookupKey();
                if (!(thisNode instanceof DocumentObject)) {
                    throw new NotAnObjectException(key);
                }
                final DocumentObject thisObject = (DocumentObject) thisNode;
                if (!thisObject.hasKey(key)) {
                    throw new UnknownLookupKeyException(key);
                }
                return thisObject.get(key);
            };
        }

        @Override
        public void visit(final ArrayLookupPathSegment arrayLookupPathSegment) {
            this.converter = thisNode -> {
                if (!(thisNode instanceof DocumentArray)) {
                    throw new NotAnArrayException();
                }
                final DocumentArray thisArray = (DocumentArray) thisNode;
                return thisArray.getValue(arrayLookupPathSegment.getLookupIndex());
            };
        }
    }

    private static class NotAnArrayException extends RuntimeException {
    }

    private static class NotAnObjectException extends RuntimeException {
        private final String lookupKey;

        private NotAnObjectException(final String lookupKey) {
            this.lookupKey = lookupKey;
        }
    }

    private static class UnknownLookupKeyException extends RuntimeException {
        private final String lookupKey;

        private UnknownLookupKeyException(final String lookupKey) {
            this.lookupKey = lookupKey;
        }
    }

}
