package com.exasol.adapter.dynamodb.mapping;

import java.io.Serializable;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.metadata.DataType;
import com.exasol.dynamodb.resultwalker.AbstractDynamodbResultWalker;
import com.exasol.dynamodb.resultwalker.DynamodbResultWalkerException;
import com.exasol.sql.expression.ValueExpression;
import com.exasol.sql.expression.rendering.ValueExpressionRenderer;
import com.exasol.sql.rendering.StringRendererConfig;

/**
 * Definition of a column mapping from DynamoDB table to Exasol Virtual Schema.
 * <p>
 * Each instance of this class represents one column in the Exasol table.
 * Objects of this class get serialized into the column adapter notes. They are
 * created using a {@link MappingFactory}. Storing the Mapping definition is
 * necessary as mapping definition files in bucketfs could change but the
 * mapping must not change until {@code REFRESH} is called.
 * </p>
 */
public abstract class AbstractColumnMappingDefinition implements Serializable {
	private static final long serialVersionUID = 48342992735371252L;
	private final String destinationName;
	private final AbstractDynamodbResultWalker resultWalker;
	private final LookupFailBehaviour lookupFailBehaviour;

	/**
	 * Creates an instance of {@link AbstractColumnMappingDefinition}.
	 * 
	 * @param destinationName
	 *            name of the Exasol column
	 * @param resultWalker
	 *            {@link AbstractDynamodbResultWalker} representing the path to the
	 *            source DynamoDB property
	 * @param lookupFailBehaviour
	 *            {@link LookupFailBehaviour} if the defined path does not exist
	 */
	public AbstractColumnMappingDefinition(final String destinationName,
			final AbstractDynamodbResultWalker resultWalker, final LookupFailBehaviour lookupFailBehaviour) {
		this.destinationName = destinationName;
		this.resultWalker = resultWalker;
		this.lookupFailBehaviour = lookupFailBehaviour;
	}

	/**
	 * Get the name of the column in the Exasol table.
	 * 
	 * @return name of the column
	 */
	public String getDestinationName() {
		return this.destinationName;
	}

	/**
	 * Get the Exasol data type.
	 * 
	 * @return Exasol data type
	 */
	public abstract DataType getDestinationDataType();

	/**
	 * Get the default value of this column.
	 * 
	 * @return {@link ValueExpression} holding default value
	 */
	public abstract ValueExpression getDestinationDefaultValue();

	public String getDestinationDefaultValueLiteral() {
		final StringRendererConfig stringRendererConfig = StringRendererConfig.createDefault();
		final ValueExpressionRenderer renderer = new ValueExpressionRenderer(stringRendererConfig);
		this.getDestinationDefaultValue().accept(renderer);
		return renderer.render();
	}

	/**
	 * Is Exasol column nullable.
	 * 
	 * @return {@code <true>} if Exasol column is nullable
	 */
	public abstract boolean isDestinationNullable();

	/**
	 * Get the {@link LookupFailBehaviour}
	 * 
	 * @return {@link LookupFailBehaviour}
	 */
	public LookupFailBehaviour getLookupFailBehaviour() {
		return this.lookupFailBehaviour;
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
			final AttributeValue dynamodbProperty = this.resultWalker.walk(dynamodbRow);
			return convertValue(dynamodbProperty);
		} catch (final DynamodbResultWalkerException | LookupColumnMappingException exception) {
			if (this.lookupFailBehaviour == LookupFailBehaviour.DEFAULT_VALUE) {
				return this.getDestinationDefaultValue();
			}
			throw exception;
		}
	}

	/**
	 * Converts the DynamoDB property into an Exasol cell value.
	 * 
	 * @param dynamodbProperty
	 *            the DynamoDB property specified using {@link #resultWalker}
	 * @return the conversion result
	 * @throws ColumnMappingException
	 */
	protected abstract ValueExpression convertValue(AttributeValue dynamodbProperty) throws ColumnMappingException;

	/**
	 * Behaviour if the requested property is not set in a given DynamoDB row.
	 */
	public enum LookupFailBehaviour {
		/**
		 * Break the whole query.
		 */
		EXCEPTION,
		/**
		 * The column specific default value is returned.
		 */
		DEFAULT_VALUE
	}

}
