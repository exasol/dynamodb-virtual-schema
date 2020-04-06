package com.exasol.adapter.dynamodb;

import java.util.List;

import com.exasol.adapter.dynamodb.queryresultschema.QueryResultTableSchema;
import com.exasol.sql.expression.ValueExpression;

/**
 * Interface for converting a list of {@link ValueExpression}s into a SQL statement for the push-down response.
 */
public interface ValueExpressionsToSqlConverter {
    /**
     * Converts a list of lists of {@link ValueExpression}s into a SQL statement.
     *
     * @param tableStructure used for creating empty row
     * @param rows           List of ordered lists of {@link ValueExpression}
     * @return SQL Statement
     */
    public String convert(final QueryResultTableSchema tableStructure, final List<List<ValueExpression>> rows);
}
