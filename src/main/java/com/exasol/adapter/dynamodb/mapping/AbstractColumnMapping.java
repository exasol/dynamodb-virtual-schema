package com.exasol.adapter.dynamodb.mapping;

/**
 * This class is an abstract basis for {@link ColumnMapping}s.
 */
abstract class AbstractColumnMapping implements ColumnMapping {
    private static final long serialVersionUID = 716520985663104045L;
    private final String exasolColumnName;

    /**
     * Creates an instance of {@link AbstractColumnMapping}
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
        if (!(other instanceof AbstractColumnMapping)) {
            return false;
        }
        final AbstractColumnMapping that = (AbstractColumnMapping) other;
        return this.exasolColumnName.equals(that.exasolColumnName);
    }

    @Override
    public int hashCode() {
        return this.exasolColumnName.hashCode();
    }
}
