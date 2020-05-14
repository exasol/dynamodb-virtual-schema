package com.exasol.adapter.dynamodb.mapping;

import java.io.Serializable;

import com.exasol.adapter.metadata.DataType;
import com.exasol.sql.expression.ValueExpression;

/**
 * This interface defines the mapping for a column in the Virtual Schema.
 *
 * <p>
 * Objects implementing this interface get serialized into the column adapter notes. They are created using a
 * {@link SchemaMappingReader}. Storing the mapping definition is necessary as mapping definition files in BucketFS
 * could be changed, but the mapping must not be changed until a {@code REFRESH} statement is called.
 * </p>
 */
public interface ColumnMapping extends Serializable {

    /**
     * Get the name of the column in the Exasol table.
     *
     * @return name of the column
     */
    public String getExasolColumnName();

    /**
     * Get the Exasol data type.
     *
     * @return Exasol data type
     */
    public DataType getExasolDataType();

    /**
     * Get the default value of this column.
     *
     * @return {@link ValueExpression} holding default value
     */
    public ValueExpression getExasolDefaultValue();

    /**
     * Describes if Exasol column is nullable.
     *
     * @return {@code <true>} if Exasol column is nullable
     */
    public boolean isExasolColumnNullable();

    /**
     * Creates a copy of this column with a different name
     * 
     * @param newExasolName new name
     * @return copy
     */
    public ColumnMapping withNewExasolName(String newExasolName);

    public void accept(ColumnMappingVisitor visitor);
}
