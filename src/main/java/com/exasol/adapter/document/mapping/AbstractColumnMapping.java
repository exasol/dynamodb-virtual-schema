package com.exasol.adapter.document.mapping;

import java.util.Objects;

/**
 * This class is an abstract basis for {@link ColumnMapping}s.
 */
abstract class AbstractColumnMapping implements ColumnMapping {
    private static final long serialVersionUID = -6273330018505208223L;
    private final String exasolColumnName;

    /**
     * Create an instance of {@link AbstractColumnMapping}
     *
     * @param exasolColumnName name of the Exasol column
     */
    protected AbstractColumnMapping(final String exasolColumnName) {
        this.exasolColumnName = exasolColumnName;
    }

    @Override
    public final String getExasolColumnName() {
        return this.exasolColumnName;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ColumnMapping)) {
            return false;
        }
        final ColumnMapping that = (ColumnMapping) other;
        return this.exasolColumnName.equals(that.getExasolColumnName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.exasolColumnName);
    }
}
