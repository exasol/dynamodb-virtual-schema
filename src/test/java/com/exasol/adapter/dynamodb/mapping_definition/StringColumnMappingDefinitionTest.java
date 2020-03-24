package com.exasol.adapter.dynamodb.mapping_definition;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dynamodb.exasol_dataframe.ExasolDataFrame;
import com.exasol.adapter.dynamodb.mapping_definition.result_walker.DynamodbResultWalker;
import com.exasol.adapter.dynamodb.mapping_definition.result_walker.IdentityDynamodbResultWalker;
import com.exasol.adapter.dynamodb.mapping_definition.result_walker.ObjectDynamodbResultWalker;
import com.exasol.adapter.metadata.DataType;

public class StringColumnMappingDefinitionTest {
	private static final String TEST_STRING = "test";
	private static final String TEST_SOURCE_COLUMN = "myColumn";
	private static final String DEST_COLUMN = "destColumn";

	private Map<String, AttributeValue> getDynamodbRow() {
		final AttributeValue stringField = new AttributeValue();
		stringField.setS(TEST_STRING);
		return Map.of(TEST_SOURCE_COLUMN, stringField);
	}

	@Test
	void testConvertRowBasic() throws AdapterException {
		final StringColumnMappingDefinition stringColumnMappingDefinition = new StringColumnMappingDefinition(
				DEST_COLUMN, TEST_STRING.length(),
				new ObjectDynamodbResultWalker(DynamodbResultWalker.LookupFailBehaviour.NULL, TEST_SOURCE_COLUMN, null),
				StringColumnMappingDefinition.OverflowBehaviour.EXCEPTION);
		final ExasolDataFrame exasolDataframe = stringColumnMappingDefinition.convertRow(getDynamodbRow());
		assertThat(exasolDataframe.toLiteral(), equalTo("'" + TEST_STRING + "'"));
	}

	@Test
	void testConvertRowOverflowTruncate() throws AdapterException {
		final StringColumnMappingDefinition stringColumnMappingDefinition = new StringColumnMappingDefinition(
				DEST_COLUMN, TEST_STRING.length() - 1,
				new ObjectDynamodbResultWalker(DynamodbResultWalker.LookupFailBehaviour.NULL, TEST_SOURCE_COLUMN, null),
				StringColumnMappingDefinition.OverflowBehaviour.TRUNCATE);
		final ExasolDataFrame exasolDataframe = stringColumnMappingDefinition.convertRow(getDynamodbRow());
		final String expected = TEST_STRING.substring(0, TEST_STRING.length() - 1);
		assertThat(exasolDataframe.toLiteral(), equalTo("'" + expected + "'"));
	}

	@Test
	void testConvertRowOverflowException() {
		final StringColumnMappingDefinition stringColumnMappingDefinition = new StringColumnMappingDefinition(
				DEST_COLUMN, TEST_STRING.length() - 1,
				new ObjectDynamodbResultWalker(DynamodbResultWalker.LookupFailBehaviour.NULL, TEST_SOURCE_COLUMN, null),
				StringColumnMappingDefinition.OverflowBehaviour.EXCEPTION);
		assertThrows(StringColumnMappingDefinition.OverflowException.class,
				() -> stringColumnMappingDefinition.convertRow(getDynamodbRow()));
	}

	@Test
	void testConvertRowAccessToNonExistingProperty() throws AdapterException {
		final StringColumnMappingDefinition stringColumnMappingDefinition = new StringColumnMappingDefinition(
				DEST_COLUMN, TEST_STRING.length() - 1,
				new ObjectDynamodbResultWalker(DynamodbResultWalker.LookupFailBehaviour.NULL, "nonExistingColumn",
						null),
				StringColumnMappingDefinition.OverflowBehaviour.EXCEPTION);
		final ExasolDataFrame exasolDataframe = stringColumnMappingDefinition.convertRow(getDynamodbRow());
		assertThat(exasolDataframe.toLiteral(), equalTo("NULL"));
	}

	@Test
	void testDestinationDataType() {
		final int stringLength = 10;
		final StringColumnMappingDefinition stringColumnMappingDefinition = new StringColumnMappingDefinition(
				DEST_COLUMN, stringLength, new IdentityDynamodbResultWalker(),
				StringColumnMappingDefinition.OverflowBehaviour.TRUNCATE);
		assertAll(
				() -> assertThat(stringColumnMappingDefinition.getDestinationDataType().getExaDataType(),
						equalTo(DataType.ExaDataType.VARCHAR)),
				() -> assertThat(stringColumnMappingDefinition.getDestinationDataType().getSize(),
						equalTo(stringLength)));
	}
}
