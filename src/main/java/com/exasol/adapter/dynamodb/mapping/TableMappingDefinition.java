package com.exasol.adapter.dynamodb.mapping;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Definition of a table mapping from DynamoDB table to Exasol Virtual Schema. Each instance of this class represents a
 * table in the Exasol Virtual Schema. Typically it also represents a DynamoDB table. But it can also represent the data
 * from a nested list or object. See {@link #isRootTable()} for details.
 */
public class TableMappingDefinition implements Serializable {
    private static final long serialVersionUID = 3568807256753213582L;
    private final String exasolName;
    private final boolean isRootTable;
    private final transient List<AbstractColumnMappingDefinition> columns; // The columns are serialized separately in
                                                                           // {@link ColumnMetadata}.

    private TableMappingDefinition(final String exasolName, final boolean isRootTable,
            final List<AbstractColumnMappingDefinition> columns) {
        this.exasolName = exasolName;
        this.isRootTable = isRootTable;
        this.columns = columns;
    }

    /**
     * Creates an instance of {@link TableMappingDefinition} from deserialized version. As the columns are transient
     * they need to be added again.
     * 
     * @param deserialized {@link TableMappingDefinition} retrieved from deserialization
     * @param columns      Columns deserialized separately
     */
    TableMappingDefinition(final TableMappingDefinition deserialized,
            final List<AbstractColumnMappingDefinition> columns) {
        this.exasolName = deserialized.exasolName;
        this.isRootTable = deserialized.isRootTable;
        this.columns = columns;
    }

    /**
     * Returns an instance of the Builder.
     *
     * @param destName    Name of the Exasol table
     * @param isRootTable see {@link #isRootTable()}
     * @return Builder for {@link TableMappingDefinition}
     */
    public static Builder builder(final String destName, final boolean isRootTable) {
        return new Builder(destName, isRootTable);
    }

    /**
     * Get the name of the Exasol table
     * 
     * @return name of the Exasol table
     */
    public String getExasolName() {
        return this.exasolName;
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
        private final String exasolName;
        private final boolean isRootTable;
        private final List<AbstractColumnMappingDefinition> columns = new ArrayList<>();

        private Builder(final String exasolName, final boolean isRootTable) {
            this.exasolName = exasolName;
            this.isRootTable = isRootTable;
        }

        /**
         * Adds a {@link AbstractColumnMappingDefinition}
         * 
         * @param columnMappingDefinition Column MappingDefinition to add
         * @return self for fluent programming interface
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
            return new TableMappingDefinition(this.exasolName, this.isRootTable,
                    Collections.unmodifiableList(this.columns));
        }
    }
}
