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
}
