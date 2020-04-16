package com.exasol.adapter.dynamodb.mapping;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.exasol.dynamodb.resultwalker.AbstractDynamodbResultWalker;

/**
 * Definition of a table mapping from DynamoDB table to Exasol Virtual Schema. Each instance of this class represents a
 * table in the Exasol Virtual Schema. Typically it also represents a DynamoDB table. But it can also represent the data
 * from a nested list or object. See {@link #isRootTable()} for details.
 */
public class TableMappingDefinition implements Serializable {
    private static final long serialVersionUID = 3568807256753213582L;
    private final String exasolName;
    private final String remoteName;
    private final transient List<AbstractColumnMappingDefinition> columns; // The columns are serialized separately in
                                                                           // {@link ColumnMetadata}.
    private final AbstractDynamodbResultWalker pathToNestedTable;

    private TableMappingDefinition(final String exasolName, final String externName,
            final List<AbstractColumnMappingDefinition> columns, final AbstractDynamodbResultWalker pathToNestedTable) {
        this.exasolName = exasolName;
        this.remoteName = externName;
        this.pathToNestedTable = pathToNestedTable;
        this.columns = columns;
    }

    TableMappingDefinition(final TableMappingDefinition deserialized,
            final List<AbstractColumnMappingDefinition> columns) {
        this.exasolName = deserialized.exasolName;
        this.remoteName = deserialized.remoteName;
        this.pathToNestedTable = deserialized.pathToNestedTable;
        this.columns = columns;
    }

    /**
     * Gives an instance of the Builder for {@link TableMappingDefinition}. This version of the builder is used for root
     * tables.
     *
     * @param destName Name of the Exasol table
     * @return {@link TableMappingDefinition.Builder}
     */
    public static Builder rootTableBuilder(final String destName, final String remoteName) {
        return new Builder(destName, remoteName, null);
    }

    /**
     * Gives an instance of the Builder for {@link TableMappingDefinition}. This version of the builder is used to
     * create tables extracted from nested lists.
     *
     * @param destName          Name of the Exasol table
     * @param pathToNestedTable Path expression within the document to the nested table
     * @return Builder for {@link TableMappingDefinition}
     */
    public static Builder nestedTableBuilder(final String destName, final String remoteName,
            final AbstractDynamodbResultWalker pathToNestedTable) {
        return new Builder(destName, remoteName, pathToNestedTable);
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
     * Get the name of the remote table that is mapped.
     *
     * @return name of the remote table
     */
    public String getRemoteName() {
        return this.remoteName;
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
        return this.pathToNestedTable == null;
    }

    /**
     * Builder for {@link TableMappingDefinition}
     */
    public static class Builder {
        private final String exasolName;
        private final String remoteName;
        private final List<AbstractColumnMappingDefinition> columns = new ArrayList<>();
        private final AbstractDynamodbResultWalker pathToNestedTable;

        private Builder(final String exasolName, final String remoteName,
                final AbstractDynamodbResultWalker pathToNestedTable) {
            this.exasolName = exasolName;
            this.remoteName = remoteName;
            this.pathToNestedTable = pathToNestedTable;
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
            return new TableMappingDefinition(this.exasolName, this.remoteName,
                    Collections.unmodifiableList(this.columns), this.pathToNestedTable);
        }
    }
}
