package com.exasol.adapter.dynamodb.mapping;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Definition of a table mapping from DynamoDB table to Exasol Virtual Schema. Each instance of this class represents on
 * table in the Exasol Virtual Schema. Typically it also represents a DynamoDB table. But it can also represent the data
 * from a nested list or object. See {@link #isRootTable()} for details.
 */
public class TableMappingDefinition {
    private final String destinationName;
    private final boolean isRootTable;
    private final List<AbstractColumnMappingDefinition> columns;

    private TableMappingDefinition(final String destinationName, final boolean isRootTable,
                                   final List<AbstractColumnMappingDefinition> columns) {
        this.destinationName = destinationName;
        this.isRootTable = isRootTable;
        this.columns = columns;
    }

    /**
     * Gives an instance of the Builder.
     *
     * @param destName    Name of the Exasol table
     * @param isRootTable see {@link #isRootTable()}
     * @return
     */
    public static Builder builder(final String destName, final boolean isRootTable) {
        return new Builder(destName, isRootTable);
    }

    /**
     * Get the name of the Exasol table
     * 
     * @return name of the Exasol table
     */
    public String getDestinationName() {
        return this.destinationName;
    }

    /**
     * Get the columns of this table
     * 
     * @return List of {@link AbstractColumnMappingDefinition}s
     */
    public List<AbstractColumnMappingDefinition> getColumns() {
        return this.columns;
    }

    /**
     * Specifies if a table has a counterpart in DynamoDB
     *
     * @return {@code <true>} if this table has an pendant in DynamoDB {@code <false>} if this table represents a nested
     *         list or map from DynamoDB
     */
    public boolean isRootTable() {
        return this.isRootTable;
    }

    /**
     * Builder for {@link TableMappingDefinition}
     */
    public static class Builder {
        private final String destName;
        private final boolean isRootTable;
        private final List<AbstractColumnMappingDefinition> columns = new ArrayList<>();

        private Builder(final String destName, final boolean isRootTable) {
            this.destName = destName;
            this.isRootTable = isRootTable;
        }

        /**
         * Adds a {@link AbstractColumnMappingDefinition}
         * 
         * @param columnMappingDefinition
         * @return self
         */
        public Builder withColumnMappingDefinition(final AbstractColumnMappingDefinition columnMappingDefinition) {
            this.columns.add(columnMappingDefinition);
            return this;
        }

        /**
         * Builds the {@link TableMappingDefinition}
         * 
         * @return {@link TableMappingDefinition}
         */
        public TableMappingDefinition build() {
            return new TableMappingDefinition(this.destName, this.isRootTable,
                    Collections.unmodifiableList(this.columns));
        }
    }
}
