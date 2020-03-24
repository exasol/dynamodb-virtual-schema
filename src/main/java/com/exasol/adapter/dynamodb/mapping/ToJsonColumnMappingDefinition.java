package com.exasol.adapter.dynamodb.mapping;

import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.metadata.DataType;
import com.exasol.cellvalue.ExasolCellValue;
import com.exasol.cellvalue.StringExasolCellValue;
import com.exasol.dynamodb.resultwalker.DynamodbResultWalker;

/**
 * Maps a property of a DynamoDB table and all it's descendants to a JSON string
 */
public class ToJsonColumnMappingDefinition extends ColumnMappingDefinition {
	private static final long serialVersionUID = 7687302490848045236L;
	private final DynamodbResultWalker sourceWalker;

	/**
	 * Constructor.
	 * 
	 * @param destinationName
	 *            name of the column in Exasol table.
	 * @param sourceWalker
	 *            source walker defining the path in DynamoDB documents.
	 */
	public ToJsonColumnMappingDefinition(final String destinationName, final DynamodbResultWalker sourceWalker) {
		super(destinationName);
		this.sourceWalker = sourceWalker;
	}

	@Override
	DataType getDestinationDataType() {
		return DataType.createVarChar(10000, DataType.ExaCharset.UTF8);
	}

	@Override
	String getDestinationDefaultValue() {
		return "";
	}

	@Override
	boolean isDestinationNullable() {
		return true;
	}

	@Override
	public ExasolCellValue convertRow(final Map<String, AttributeValue> dynamodbRow)
			throws DynamodbResultWalker.DynamodbResultWalkerException {
		final AttributeValue source = this.sourceWalker.walk(dynamodbRow);
		// TODO convert to json
		return new StringExasolCellValue("dummy value");
	}
}
