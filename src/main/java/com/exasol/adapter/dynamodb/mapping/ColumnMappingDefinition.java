package com.exasol.adapter.dynamodb.mapping;

import java.io.Serializable;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.metadata.DataType;
import com.exasol.sql.expression.ValueExpression;

/**
 * Definition of a column mapping from DynamoDB table to Exasol Virtual Schema. Each instance of this class represents
 * one column in the Exasol table.
 */
public interface ColumnMappingDefinition extends Serializable {

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
     * Get the string representation of the Exasol column default value literal.
     * 
     * @return default value string
     */
    public String getExasolDefaultValueLiteral();

    /**
     * Describes if Exasol column is nullable.
     *
     * @return {@code <true>} if Exasol column is nullable
     */
    public boolean isExasolColumnNullable();

    /**
     * Get the {@link LookupFailBehaviour}
     *
     * @return {@link LookupFailBehaviour}
     */
    public LookupFailBehaviour getLookupFailBehaviour();

    /**
     * Gives the path to the property in the remote document that is mapped by this definition.
     * 
     * @return path to property
     */
    public DocumentPathExpression getPathToSourceProperty();

    public void accept(ColumnMappingDefinitionVisitor visitor);
}
