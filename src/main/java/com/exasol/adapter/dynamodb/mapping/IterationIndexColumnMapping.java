package com.exasol.adapter.dynamodb.mapping;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.metadata.DataType;
import com.exasol.sql.expression.IntegerLiteral;
import com.exasol.sql.expression.ValueExpression;

/**
 * This class defines a column that maps the array index of a nested list. Such columns are useful for nested tables
 * that do not have an key.
 */
public class IterationIndexColumnMapping extends AbstractColumnMapping {
    private static final long serialVersionUID = -5048863774264562514L;
    private final DocumentPathExpression tablesPath;

    /**
     * Creates an instance of {@link IterationIndexColumnMapping}.
     *
     * @param exasolColumnName name of the Exasol column
     * @param tablesPath       the path to the array that's row index is modeled using this column
     */
    IterationIndexColumnMapping(final String exasolColumnName, final DocumentPathExpression tablesPath) {
        super(exasolColumnName);
        this.tablesPath = tablesPath;
    }

    @Override
    public DataType getExasolDataType() {
        return DataType.createDecimal(9, 0);
    }

    @Override
    public ValueExpression getExasolDefaultValue() {
        return IntegerLiteral.of(-1);// The default value is never used anyway
    }

    @Override
    public boolean isExasolColumnNullable() {
        return false;
    }

    @Override
    public ColumnMapping withNewExasolName(final String newExasolName) {
        return new IterationIndexColumnMapping(newExasolName, this.tablesPath);
    }

    @Override
    public void accept(final ColumnMappingVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Gives the path to the array that's row index is modeled using this column
     * 
     * @return path
     */
    public DocumentPathExpression getTablesPath() {
        return this.tablesPath;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof IterationIndexColumnMapping)) {
            return false;
        }
        if (!super.equals(other)) {
            return false;
        }
        final IterationIndexColumnMapping that = (IterationIndexColumnMapping) other;
        return this.tablesPath.equals(that.tablesPath);
    }

    @Override
    public int hashCode() {
        return this.tablesPath.hashCode() + super.hashCode() * 31;
    }
}
