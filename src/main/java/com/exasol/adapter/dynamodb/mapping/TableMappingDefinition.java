package com.exasol.adapter.dynamodb.mapping;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.exasol.dynamodb.resultwalker.AbstractDynamodbResultWalker;

/**
 * Definition of a table mapping from DynamoDB table to Exasol Virtual Schema. Each instance of this class represents a
 * table in the Exasol Virtual Schema. Typically it also represents a DynamoDB table. But it can also represent the data
 * from a nested list or object. See {@link #isRootTable()} for details.
 */
public class TableMappingDefinition {
    private final String exasolName;
    private final List<AbstractColumnMappingDefinition> columns;
    private final AbstractDynamodbResultWalker pathToNestedTable;

    private TableMappingDefinition(final String exasolName, final List<AbstractColumnMappingDefinition> columns,
            final AbstractDynamodbResultWalker pathToNestedTable) {
        this.exasolName = exasolName;
        this.columns = columns;
        this.pathToNestedTable = pathToNestedTable;
    }

    /**
     * Gives an instance of the Builder for {@link TableMappingDefinition}. This version of the builder is used for root
     * tables.
     *
     * @param destName Name of the Exasol table
     * @return {@link TableMappingDefinition.Builder}
     */
    public static Builder builder(final String destName) {
        return new Builder(destName, null);
    }

    /**
     * Gives an instance of the Builder for {@link TableMappingDefinition}. This version of the builder is used to
     * create tables extracted from nested lists.
     *
     * @param destName Name of the Exasol table
     * @return Builder for {@link TableMappingDefinition}
     */
    public static Builder builder(final String destName, final AbstractDynamodbResultWalker pathToNestedTable) {
        return new Builder(destName, pathToNestedTable);
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
        return this.pathToNestedTable == null;
    }

    /**
     * Builder for {@link TableMappingDefinition}
     */
    public static class Builder {
        private final String exasolName;
        private final List<AbstractColumnMappingDefinition> columns = new ArrayList<>();
        private final AbstractDynamodbResultWalker pathToNestedTable;

        private Builder(final String exasolName, final AbstractDynamodbResultWalker pathToNestedTable) {
            this.exasolName = exasolName;
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
            return new TableMappingDefinition(this.exasolName, Collections.unmodifiableList(this.columns),
                    this.pathToNestedTable);
        }
    }
}
