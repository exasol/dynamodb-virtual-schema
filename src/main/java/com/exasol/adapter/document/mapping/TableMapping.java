package com.exasol.adapter.document.mapping;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.exasol.adapter.document.documentpath.DocumentPathExpression;

/**
 * Definition of a table mapping from DynamoDB table to Exasol Virtual Schema. Each instance of this class represents a
 * table in the Exasol Virtual Schema. Typically it also represents a DynamoDB table. But it can also represent the data
 * from a nested list or object. See {@link #isRootTable()} for details.
 */
public class TableMapping implements Serializable {
    private static final long serialVersionUID = 4768289714640213806L;
    private final String exasolName;
    private final String remoteName;
    private final transient List<ColumnMapping> columns; // The columns are serialized separately in
                                                         // {@link ColumnMetadata}.
    private final DocumentPathExpression pathInRemoteTable;

    public TableMapping(final String exasolName, final String remoteName, final List<ColumnMapping> columns,
            final DocumentPathExpression pathInRemoteTable) {
        this.exasolName = exasolName;
        this.remoteName = remoteName;
        this.pathInRemoteTable = pathInRemoteTable;
        this.columns = columns;
    }

    TableMapping(final TableMapping deserialized, final List<ColumnMapping> columns) {
        this.exasolName = deserialized.exasolName;
        this.remoteName = deserialized.remoteName;
        this.pathInRemoteTable = deserialized.pathInRemoteTable;
        this.columns = columns;
    }

    /**
     * Get an instance of the Builder for {@link TableMapping}. This version of the builder is used for root tables.
     *
     * @param destinationName Name of the Exasol table
     * @return {@link TableMapping.Builder}
     */
    public static Builder rootTableBuilder(final String destinationName, final String remoteName) {
        final DocumentPathExpression emptyPath = DocumentPathExpression.empty();
        return new Builder(destinationName, remoteName, emptyPath);
    }

    /**
     * Get an instance of the builder for {@link TableMapping}. This version of the builder is used to create tables
     * extracted from nested lists.
     *
     * @param destinationName   Name of the Exasol table
     * @param remoteName        Name of the remote table
     * @param pathInRemoteTable Path expression within the document to a nested list that is mapped to a table
     * @return Builder for {@link TableMapping}
     */
    public static Builder nestedTableBuilder(final String destinationName, final String remoteName,
            final DocumentPathExpression pathInRemoteTable) {
        return new Builder(destinationName, remoteName, pathInRemoteTable);
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
     * Get the path to the nested table that is mapped by this table.
     * 
     * @return path to nested list. Empty path expression if this tables maps a root document.
     */

    public DocumentPathExpression getPathInRemoteTable() {
        return this.pathInRemoteTable;
    }

    /**
     * Get the columns of this table
     * 
     * @return List of {@link ColumnMapping}s
     */
    public List<ColumnMapping> getColumns() {
        return this.columns;
    }

    /**
     * Specifies if a table has a counterpart in DynamoDB
     *
     * @return {@code <true>} if this table has an pendant in DynamoDB {@code <false>} if this table represents a nested
     *         list or map from DynamoDB
     */
    public boolean isRootTable() {
        return this.pathInRemoteTable.size() == 0;
    }

    /**
     * Builder for {@link TableMapping}
     */
    public static class Builder {
        private final String exasolName;
        private final String remoteName;
        private final List<ColumnMapping> columns = new ArrayList<>();
        private final DocumentPathExpression pathToNestedTable;

        private Builder(final String exasolName, final String remoteName,
                final DocumentPathExpression pathToNestedTable) {
            this.exasolName = exasolName;
            this.remoteName = remoteName;
            this.pathToNestedTable = pathToNestedTable;
        }

        /**
         * Add a {@link ColumnMapping}
         * 
         * @param columnMapping Column MappingDefinition to add
         * @return self for fluent programming interface
         */
        public Builder withColumnMappingDefinition(final ColumnMapping columnMapping) {
            this.columns.add(columnMapping);
            return this;
        }

        /**
         * Builds the {@link TableMapping}
         * 
         * @return {@link TableMapping}
         */
        public TableMapping build() {
            return new TableMapping(this.exasolName, this.remoteName, Collections.unmodifiableList(this.columns),
                    this.pathToNestedTable);
        }
    }
}
