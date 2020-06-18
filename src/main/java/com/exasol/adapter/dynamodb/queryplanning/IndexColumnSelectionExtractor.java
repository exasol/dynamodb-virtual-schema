package com.exasol.adapter.dynamodb.queryplanning;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.exasol.adapter.dynamodb.mapping.IterationIndexColumnMapping;
import com.exasol.adapter.dynamodb.querypredicate.ColumnLiteralComparisonPredicate;
import com.exasol.adapter.dynamodb.querypredicate.QueryPredicate;
import com.exasol.adapter.dynamodb.querypredicate.normalizer.DnfAnd;
import com.exasol.adapter.dynamodb.querypredicate.normalizer.DnfComparison;
import com.exasol.adapter.dynamodb.querypredicate.normalizer.DnfNormalizer;
import com.exasol.adapter.dynamodb.querypredicate.normalizer.DnfOr;

/**
 * This class extracts the predicates on {@link IterationIndexColumnMapping}s from a selection. Selections on these
 * predicates can't be pushed down as their value is determined during the schema mapping.
 */
public class IndexColumnSelectionExtractor {
    private final DnfNormalizer dnfNormalizer;

    /**
     * Create an instance of {@link IndexColumnSelectionExtractor}.
     */
    public IndexColumnSelectionExtractor() {
        this.dnfNormalizer = new DnfNormalizer();
    }

    /**
     * Extract the predicates on {@link IterationIndexColumnMapping}s from a selection.
     * 
     * @param selection to spli
     * @return {@link Result} containing one selection with the predicates on {@link IterationIndexColumnMapping}s and
     *         another on the other columns.
     */
    public Result extractIndexColumnSelection(final QueryPredicate selection) {
        final DnfOr dnfOr = this.dnfNormalizer.normalize(selection);
        final List<SplitDnfAnd> splitDnfAnds = dnfOr.getOperands().stream()
                .map(this::splitUpDnfAnd).collect(Collectors.toList());
        if (splitDnfAnds.isEmpty()) {
            return new Result(new DnfOr(Collections.emptySet()), new DnfOr(Collections.emptySet()));
        }
        final Set<Set<DnfComparison>> indexComparisonDnfAnds = splitDnfAnds.stream()
                .map(splitDnfAnd -> splitDnfAnd.indexComparisons).collect(Collectors.toSet());
        final Set<Set<DnfComparison>> nonIndexComparisonAnds = splitDnfAnds.stream()
                .map(splitDnfAnd -> splitDnfAnd.nonIndexComparisons).collect(Collectors.toSet());
        if (indexComparisonDnfAnds.size() == 1 || nonIndexComparisonAnds.size() == 1) {// All ANDs have the same index
                                                                                       // predicates
            final DnfOr indexSelection = wrapInDnfOr(indexComparisonDnfAnds);
            final DnfOr nonIndexSelection = wrapInDnfOr(nonIndexComparisonAnds);
            return new Result(indexSelection, nonIndexSelection);
        } else {
            throw new UnsupportedOperationException(
                    "This query combines comparisons on INDEX columns and other columns in a way, so that the selection can't be split up.");
        }
    }

    private DnfOr wrapInDnfOr(final Set<Set<DnfComparison>> nonIndexComparisonAnds) {
        return new DnfOr(nonIndexComparisonAnds.stream().map(DnfAnd::new).collect(Collectors.toSet()));
    }

    private SplitDnfAnd splitUpDnfAnd(final DnfAnd dnfAnd) {
        final SplitDnfAnd result = new SplitDnfAnd();
        for (final DnfComparison comparison : dnfAnd.getOperands()) {
            if (comparison.getComparisonPredicate().getComparedColumns().stream()
                    .anyMatch(column -> column instanceof IterationIndexColumnMapping)) {
                if (!(comparison.getComparisonPredicate() instanceof ColumnLiteralComparisonPredicate)) {
                    throw new UnsupportedOperationException(
                            "INDEX columns can only be compared to literals. Please change your SQL query.");
                }
                result.indexComparisons.add(comparison);
            } else {
                result.nonIndexComparisons.add(comparison);
            }
        }
        return result;
    }

    public static class Result {
        private final DnfOr indexSelection;
        private final DnfOr nonIndexSelection;

        public Result(final DnfOr indexSelection, final DnfOr nonIndexSelection) {
            this.indexSelection = indexSelection;
            this.nonIndexSelection = nonIndexSelection;
        }

        public DnfOr getIndexSelection() {
            return this.indexSelection;
        }

        public DnfOr getNonIndexSelection() {
            return this.nonIndexSelection;
        }
    }

    private static class SplitDnfAnd {
        final Set<DnfComparison> nonIndexComparisons = new HashSet<>();
        final Set<DnfComparison> indexComparisons = new HashSet<>();
    }
}
