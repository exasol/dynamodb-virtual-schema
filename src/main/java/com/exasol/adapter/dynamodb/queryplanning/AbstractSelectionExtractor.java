package com.exasol.adapter.dynamodb.queryplanning;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.exasol.adapter.dynamodb.mapping.IterationIndexColumnMapping;
import com.exasol.adapter.dynamodb.querypredicate.QueryPredicate;
import com.exasol.adapter.dynamodb.querypredicate.normalizer.DnfAnd;
import com.exasol.adapter.dynamodb.querypredicate.normalizer.DnfComparison;
import com.exasol.adapter.dynamodb.querypredicate.normalizer.DnfNormalizer;
import com.exasol.adapter.dynamodb.querypredicate.normalizer.DnfOr;

/**
 * This class provides the abstract basis for splitting up a selection into two selections that can be combined with an
 * AND. The decision which comparison belongs to which selection is delegated to the concrete implementation by the
 * abstract {@link #matchComparison(DnfComparison)} method.
 */
public abstract class AbstractSelectionExtractor {
    protected final DnfNormalizer dnfNormalizer;

    public AbstractSelectionExtractor() {
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
        final List<SplitDnfAnd> splitDnfAnds = dnfOr.getOperands().stream().map(this::splitUpDnfAnd)
                .collect(Collectors.toList());
        if (splitDnfAnds.isEmpty()) {
            return new Result(new DnfOr(Collections.emptySet()), new DnfOr(Collections.emptySet()));
        }
        final Set<Set<DnfComparison>> indexComparisonDnfAnds = splitDnfAnds.stream()
                .map(splitDnfAnd -> splitDnfAnd.matchedComparisons).collect(Collectors.toSet());
        final Set<Set<DnfComparison>> nonIndexComparisonAnds = splitDnfAnds.stream()
                .map(splitDnfAnd -> splitDnfAnd.notMatchedComparisons).collect(Collectors.toSet());
        if (indexComparisonDnfAnds.size() == 1 || nonIndexComparisonAnds.size() == 1) {// All ANDs have the same index
                                                                                       // predicates
            final DnfOr indexSelection = wrapInDnfOr(indexComparisonDnfAnds);
            final DnfOr nonIndexSelection = wrapInDnfOr(nonIndexComparisonAnds);
            return new Result(indexSelection, nonIndexSelection);
        } else {
            throw new UnsupportedOperationException(
                    "This query combines selections on columns in a way, so that the selection can't be split up.");
        }
    }

    private DnfOr wrapInDnfOr(final Set<Set<DnfComparison>> nonIndexComparisonAnds) {
        return new DnfOr(nonIndexComparisonAnds.stream().map(DnfAnd::new).collect(Collectors.toSet()));
    }

    private SplitDnfAnd splitUpDnfAnd(final DnfAnd dnfAnd) {
        final SplitDnfAnd result = new SplitDnfAnd();
        for (final DnfComparison comparison : dnfAnd.getOperands()) {
            if (matchComparison(comparison)) {
                result.matchedComparisons.add(comparison);
            } else {
                result.notMatchedComparisons.add(comparison);
            }
        }
        return result;
    }

    /**
     * Separate the comparisons in two groups. The matched and the unmatched group.
     * 
     * @param comparison comparison to test
     * @return {@code true} if comparison shall be included the {@link Result#selectedSelection}.
     */
    protected abstract boolean matchComparison(DnfComparison comparison);

    public static class Result {
        private final DnfOr selectedSelection;
        private final DnfOr remainingSelection;

        public Result(final DnfOr selectedSelection, final DnfOr remainingSelection) {
            this.selectedSelection = selectedSelection;
            this.remainingSelection = remainingSelection;
        }

        public DnfOr getSelectedSelection() {
            return this.selectedSelection;
        }

        public DnfOr getRemainingSelection() {
            return this.remainingSelection;
        }
    }

    private static class SplitDnfAnd {
        final Set<DnfComparison> notMatchedComparisons = new HashSet<>();
        final Set<DnfComparison> matchedComparisons = new HashSet<>();
    }
}
