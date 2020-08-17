package com.exasol.adapter.document.documentpath;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class eliminates path expressions that are redundant when used as a projection expression.
 *
 * For example {@code a} and {@code a.b} are reduced to {@code a} as {@code a} includes {@code a.b}.
 */
public class RedundantPathEliminator {
    private static final RedundantPathEliminator INSTANCE = new RedundantPathEliminator();

    /**
     * Empty constructor to hide public default.
     */
    private RedundantPathEliminator() {
        // empty on purpose.
    }

    /**
     * Get a singleton instance of {@link RedundantPathEliminator}.
     *
     * @return instance of {@link RedundantPathEliminator}
     */
    public static RedundantPathEliminator getInstance() {
        return INSTANCE;
    }

    /**
     * Eliminate path expressions that are redundant when used as a projection expression.
     *
     * @param paths collection of paths
     * @return Set with redundancy free paths
     */
    public Set<DocumentPathExpression> removeRedundantPaths(final Collection<DocumentPathExpression> paths) {
        return removeRedundantPaths(paths.stream());
    }

    /**
     * Eliminate path expressions that are redundant when used as a projection expression.
     * 
     * @param paths collection of paths
     * @return Set with redundancy free paths
     * 
     * @implNote The basic idea of this algorithm is to iterate over the paths with an increasing path length. In the
     *           first iteration only the paths with a length of 0 are considered. In each iteration the algorithm
     *           removes all paths that are already included in the result from allPaths.
     */
    public Set<DocumentPathExpression> removeRedundantPaths(final Stream<DocumentPathExpression> paths) {
        final List<DocumentPathExpression> allPaths = paths.collect(Collectors.toCollection(LinkedList::new));
        final Set<DocumentPathExpression> redundancyFreePaths = new HashSet<>(allPaths.size() * 2);
        int currentPathLength = 0;

        while (!allPaths.isEmpty()) {
            int nextPathLength = Integer.MAX_VALUE;
            final Iterator<DocumentPathExpression> pathIterator = allPaths.iterator();
            while (pathIterator.hasNext()) {
                final DocumentPathExpression path = pathIterator.next();
                final DocumentPathExpression subPath = currentPathLength == 0 ? DocumentPathExpression.empty()
                        : path.getSubPath(0, currentPathLength - 1);
                if (redundancyFreePaths.contains(subPath)) {
                    /*
                     * A more generic path is already included. --> remove this path.
                     */
                    pathIterator.remove();
                } else if (path.size() == currentPathLength) {
                    /*
                     * This path is not include and has the current length --> add
                     */
                    redundancyFreePaths.add(path);
                    pathIterator.remove();
                    /*
                     * Here we also queue a check on the next path length to remove that paths that contain this path.
                     */
                    nextPathLength = Math.min(nextPathLength, currentPathLength + 1);
                } else {
                    /*
                     * This path is not included but has not the current length. --> Will be considered in a future
                     * iteration. We set nextPathLength to skip pathLengths with no paths.
                     */
                    nextPathLength = Math.min(nextPathLength, path.size());
                }
            }
            currentPathLength = nextPathLength;
        }
        return redundancyFreePaths;
    }
}
