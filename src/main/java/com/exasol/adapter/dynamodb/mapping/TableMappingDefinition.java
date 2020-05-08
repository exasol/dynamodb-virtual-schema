package com.exasol.adapter.dynamodb.mapping;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;

/**
 * Definition of a table mapping from DynamoDB table to Exasol Virtual Schema. Each instance of this class represents a
 * table in the Exasol Virtual Schema. Typically it also represents a DynamoDB table. But it can also represent the data
 * from a nested list or object. See {@link #isRootTable()} for details.
 */
public class TableMappingDefinition implements Serializable {
    private static final long serialVersionUID = 3568807256753213582L;
    private final String exasolName;
    private final String remoteName;
    private final transient List<ColumnMappingDefinition> columns; // The columns are serialized separately in
                                                                   // {@link ColumnMetadata}.

    private final DocumentPathExpression pathToNestedTable;

    TableMappingDefinition(final String exasolName, final String remoteName,
            final List<ColumnMappingDefinition> columns, final DocumentPathExpression pathToNestedTable) {
        this.exasolName = exasolName;
        this.remoteName = remoteName;
        this.pathToNestedTable = pathToNestedTable;
        this.columns = columns;
    }

    TableMappingDefinition(final TableMappingDefinition deserialized, final List<ColumnMappingDefinition> columns) {
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
        final DocumentPathExpression emptyPath = new DocumentPathExpression.Builder().build();
        return new Builder(destName, remoteName, emptyPath);
    }

    /**
     * Gives an instance of the Builder for {@link TableMappingDefinition}. This version of the builder is used to
     * create tables extracted from nested lists.
     *
     * @param destName          Name of the Exasol table
     * @param remoteName        Name of the remote table
     * @param pathToNestedTable Path expression within the document to the nested table
     * @return Builder for {@link TableMappingDefinition}
     */
    public static Builder nestedTableBuilder(final String destName, final String remoteName,
            final DocumentPathExpression pathToNestedTable) {
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
     * Gives the path to the nested table.
     * 
     * @return path to nested table. Empty path expression if this is a root table.
     */
    public DocumentPathExpression getPathToNestedTable() {
        return this.pathToNestedTable;
    }

    /**
     * Get the columns of this table
     * 
     * @return List of {@link ColumnMappingDefinition}s
     */
    public List<ColumnMappingDefinition> getColumns() {
        return this.columns;
    }

    /**
     * Specifies if a table has a counterpart in DynamoDB
     *
     * @return {@code <true>} if this table has an pendant in DynamoDB {@code <false>} if this table represents a nested
     *         list or map from DynamoDB
     */
    public boolean isRootTable() {
        return this.pathToNestedTable.size() == 0;
    }

    /**
     * Builder for {@link TableMappingDefinition}
     */
    public static class Builder {
        private final String exasolName;
        private final String remoteName;
        private final List<ColumnMappingDefinition> columns = new ArrayList<>();
        private final DocumentPathExpression pathToNestedTable;

        private Builder(final String exasolName, final String remoteName,
                final DocumentPathExpression pathToNestedTable) {
            this.exasolName = exasolName;
            this.remoteName = remoteName;
            this.pathToNestedTable = pathToNestedTable;
        }

        /**
         * Adds a {@link ColumnMappingDefinition}
         * 
         * @param columnMappingDefinition Column MappingDefinition to add
         * @return self for fluent programming interface
         */
        public Builder withColumnMappingDefinition(final ColumnMappingDefinition columnMappingDefinition) {
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
