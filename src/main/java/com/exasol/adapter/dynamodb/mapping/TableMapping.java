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
public class TableMapping implements Serializable {
    private static final long serialVersionUID = -3340645367432789767L;
    private final String exasolName;
    private final String remoteName;
    private final transient List<ColumnMapping> columns; // The columns are serialized separately in
                                                         // {@link ColumnMetadata}.

    private final DocumentPathExpression pathToNestedTable;

    TableMapping(final String exasolName, final String remoteName, final List<ColumnMapping> columns,
            final DocumentPathExpression pathToNestedTable) {
        this.exasolName = exasolName;
        this.remoteName = remoteName;
        this.pathToNestedTable = pathToNestedTable;
        this.columns = columns;
    }

    TableMapping(final TableMapping deserialized, final List<ColumnMapping> columns) {
        this.exasolName = deserialized.exasolName;
        this.remoteName = deserialized.remoteName;
        this.pathToNestedTable = deserialized.pathToNestedTable;
        this.columns = columns;
    }

    /**
     * Gives an instance of the Builder for {@link TableMapping}. This version of the builder is used for root tables.
     *
     * @param destName Name of the Exasol table
     * @return {@link TableMapping.Builder}
     */
    public static Builder rootTableBuilder(final String destName, final String remoteName) {
        final DocumentPathExpression emptyPath = new DocumentPathExpression.Builder().build();
        return new Builder(destName, remoteName, emptyPath);
    }

    /**
     * m Gives an instance of the Builder for {@link TableMapping}. This version of the builder is used to create tables
     * extracted from nested lists.
     *
     * @param destName          Name of the Exasol table
     * @param remoteName        Name of the remote table
     * @param pathToNestedTable Path expression within the document to the nested table
     * @return Builder for {@link TableMapping}
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
        return this.pathToNestedTable.size() == 0;
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
         * Adds a {@link ColumnMapping}
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