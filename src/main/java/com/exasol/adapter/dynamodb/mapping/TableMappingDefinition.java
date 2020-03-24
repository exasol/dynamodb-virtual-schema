package com.exasol.adapter.dynamodb.mapping;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.TableMetadata;

/**
 * Definition of a table mapping from DynamoDB table to Exasol Virtual Schema.
 * Each instance of this class represents on table in the Exasol Virtual Schema.
 * Typically it also represents a DynamoDB table. But it can also represent the
 * data from a nested list or object. See {@link #isRootTable() for details}
 */
public class TableMappingDefinition {
	private final String destName;
	private final boolean isRootTable;
	private final List<ColumnMappingDefinition> columns;

	private TableMappingDefinition(final String destName, final boolean isRootTable,
			final List<ColumnMappingDefinition> columns) {
		this.destName = destName;
		this.isRootTable = isRootTable;
		this.columns = columns;
	}

	public String getDestName() {
		return this.destName;
	}

	public List<ColumnMappingDefinition> getColumns() {
		return this.columns;
	}

	/**
	 * Gives an instance of the Builder.
	 * 
	 * @param destName
	 *            Name of the Exasol table
	 * @param isRootTable
	 *            see {@link #isRootTable()}
	 * @return
	 */
	public static Builder builder(final String destName, final boolean isRootTable) {
		return new Builder(destName, isRootTable);
	}

	/**
	 * Specifies if a table has a pendant in DynamoDB
	 *
	 * @return {@code <true>} if this table has an pendant in DynamoDB
	 *         {@code <false>} if this table represents a nested list or map from
	 *         DynamoDB
	 */
	boolean isRootTable() {
		return this.isRootTable;
	}

	/**
	 * Builder for {@link TableMappingDefinition}
	 */
	public static class Builder {
		private final String destName;
		private final boolean isRootTable;
		private final List<ColumnMappingDefinition> columns = new ArrayList<>();
		private Builder(final String destName, final boolean isRootTable) {
			this.destName = destName;
			this.isRootTable = isRootTable;
		}

		/**
		 * Adds a {@link ColumnMappingDefinition}
		 * 
		 * @param columnMappingDefinition
		 * @return self
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
			return new TableMappingDefinition(this.destName, this.isRootTable,
					Collections.unmodifiableList(this.columns));
		}
	}
}
