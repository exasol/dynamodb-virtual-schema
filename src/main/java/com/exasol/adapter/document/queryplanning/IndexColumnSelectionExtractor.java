package com.exasol.adapter.document.queryplanning;

import com.exasol.adapter.document.mapping.IterationIndexColumnMapping;
import com.exasol.adapter.document.querypredicate.ColumnLiteralComparisonPredicate;
import com.exasol.adapter.document.querypredicate.normalizer.DnfComparison;

/**
 * This class extracts the predicates on {@link IterationIndexColumnMapping}s from a selection. Selections on these
 * predicates can't be pushed down as their value is determined during the schema mapping.
 */
public class IndexColumnSelectionExtractor extends AbstractSelectionExtractor {

    /**
     * Create an instance of {@link IndexColumnSelectionExtractor}.
     */
    public IndexColumnSelectionExtractor() {
        super();
    }

    @Override
    protected boolean matchComparison(final DnfComparison comparison) {
        if (comparison.getComparisonPredicate().getComparedColumns().stream()
                .anyMatch(column -> column instanceof IterationIndexColumnMapping)) {
            if (!(comparison.getComparisonPredicate() instanceof ColumnLiteralComparisonPredicate)) {
                throw new UnsupportedOperationException(
                        "INDEX columns can only be compared to literals. Please change your SQL query.");
            } else {
                return true;
            }
        } else {
            return false;
        }
    }
}
