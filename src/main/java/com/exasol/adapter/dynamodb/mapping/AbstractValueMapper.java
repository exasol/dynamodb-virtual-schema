package com.exasol.adapter.dynamodb.mapping;

import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.AdapterException;
import com.exasol.dynamodb.resultwalker.DynamodbResultWalkerException;
import com.exasol.sql.expression.ValueExpression;

public abstract class ValueMapper {
	private final AbstractColumnMappingDefinition column;

	public ValueMapper(final AbstractColumnMappingDefinition column) {
		this.column = column;
	}

	/**
	 * Extracts this column's value from DynamoDB's result row.
	 *
	 * @param dynamodbRow
	 * @return {@link ValueExpression}
	 * @throws AdapterException
	 */
	public ValueExpression convertRow(final Map<String, AttributeValue> dynamodbRow)
			throws DynamodbResultWalkerException, ColumnMappingException {
		try {
			final AttributeValue dynamodbProperty = this.column.getResultWalker().walk(dynamodbRow);
			return this.convertValue(dynamodbProperty);
		} catch (final DynamodbResultWalkerException | LookupColumnMappingException exception) {
			if (this.column
					.getLookupFailBehaviour() == AbstractColumnMappingDefinition.LookupFailBehaviour.DEFAULT_VALUE) {
				return this.column.getDestinationDefaultValue();
			}
			throw exception;
		}
	}

	/**
	 * Converts the DynamoDB property into an Exasol cell value.
	 *
	 * @param dynamodbProperty
	 *            the DynamoDB property to be converted
	 * @return the conversion result
	 * @throws ColumnMappingException
	 */
	protected abstract ValueExpression convertValue(AttributeValue dynamodbProperty) throws ColumnMappingException;
}
