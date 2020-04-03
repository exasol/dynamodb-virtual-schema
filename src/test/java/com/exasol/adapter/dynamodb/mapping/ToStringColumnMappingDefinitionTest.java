package com.exasol.adapter.dynamodb.mapping;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.metadata.DataType;
import com.exasol.dynamodb.resultwalker.IdentityDynamodbResultWalker;
import com.exasol.dynamodb.resultwalker.ObjectDynamodbResultWalker;
import com.exasol.sql.expression.ValueExpression;

/**
 * Tests for {@link ToStringColumnMappingDefinition}
 */
public class ToStringColumnMappingDefinitionTest {
	private static final String TEST_STRING = "test";
	private static final int TEST_NUMBER = 42;
	private static final String TEST_SOURCE_COLUMN = "myColumn";
	private static final String DEST_COLUMN = "destColumn";

	private Map<String, AttributeValue> getDynamodbStringRow() {
		final AttributeValue stringField = new AttributeValue();
		stringField.setS(TEST_STRING);
		return Map.of(TEST_SOURCE_COLUMN, stringField);
	}

	private Map<String, AttributeValue> getDynamodbNumberRow() {
		final AttributeValue stringField = new AttributeValue();
		stringField.setN(String.valueOf(TEST_NUMBER));
		return Map.of(TEST_SOURCE_COLUMN, stringField);
	}

	private Map<String, AttributeValue> getDynamodbListRow() {
		final AttributeValue stringField = new AttributeValue();
		stringField.setL(Collections.emptyList());
		return Map.of(TEST_SOURCE_COLUMN, stringField);
	}

	@Test
	void testConvertStringRowBasic() throws AdapterException {
		final ToStringColumnMappingDefinition toStringColumnMappingDefinition = new ToStringColumnMappingDefinition(
				DEST_COLUMN, TEST_STRING.length(), new ObjectDynamodbResultWalker(TEST_SOURCE_COLUMN, null),
				AbstractColumnMappingDefinition.LookupFailBehaviour.DEFAULT_VALUE,
				ToStringColumnMappingDefinition.OverflowBehaviour.EXCEPTION);
		final ValueExpression exasolCellValue = toStringColumnMappingDefinition.convertRow(getDynamodbStringRow());
		assertThat(exasolCellValue.toString(), equalTo(TEST_STRING));
	}

	@Test
	void testConvertNumberRowBasic() throws AdapterException {
		final ToStringColumnMappingDefinition toStringColumnMappingDefinition = new ToStringColumnMappingDefinition(
				DEST_COLUMN, String.valueOf(TEST_NUMBER).length(),
				new ObjectDynamodbResultWalker(TEST_SOURCE_COLUMN, null),
				AbstractColumnMappingDefinition.LookupFailBehaviour.DEFAULT_VALUE,
				ToStringColumnMappingDefinition.OverflowBehaviour.EXCEPTION);
		final ValueExpression exasolCellValue = toStringColumnMappingDefinition.convertRow(getDynamodbNumberRow());
		assertThat(exasolCellValue.toString(), equalTo(String.valueOf(TEST_NUMBER)));
	}

	@Test
	void testConvertUnsupportedDynamodbType() {
		final ToStringColumnMappingDefinition toStringColumnMappingDefinition = new ToStringColumnMappingDefinition(
				DEST_COLUMN, 2, new ObjectDynamodbResultWalker(TEST_SOURCE_COLUMN, null),
				AbstractColumnMappingDefinition.LookupFailBehaviour.EXCEPTION,
				ToStringColumnMappingDefinition.OverflowBehaviour.EXCEPTION);
		final ColumnMappingException exception = assertThrows(ColumnMappingException.class,
				() -> toStringColumnMappingDefinition.convertRow(getDynamodbListRow()));
		assertThat(exception.getMessage(), equalTo("The DynamoDB type List cant't be converted to string."));
	}

	@Test
	void testConvertRowOverflowTruncate() throws AdapterException {
		final ToStringColumnMappingDefinition toStringColumnMappingDefinition = new ToStringColumnMappingDefinition(
				DEST_COLUMN, TEST_STRING.length() - 1, new ObjectDynamodbResultWalker(TEST_SOURCE_COLUMN, null),
				AbstractColumnMappingDefinition.LookupFailBehaviour.DEFAULT_VALUE,
				ToStringColumnMappingDefinition.OverflowBehaviour.TRUNCATE);
		final ValueExpression exasolCellValue = toStringColumnMappingDefinition.convertRow(getDynamodbStringRow());
		final String expected = TEST_STRING.substring(0, TEST_STRING.length() - 1);
		assertThat(exasolCellValue.toString(), equalTo(expected));
	}

	@Test
	void testConvertRowOverflowException() {
		final ToStringColumnMappingDefinition toStringColumnMappingDefinition = new ToStringColumnMappingDefinition(
				DEST_COLUMN, TEST_STRING.length() - 1, new ObjectDynamodbResultWalker(TEST_SOURCE_COLUMN, null),
				AbstractColumnMappingDefinition.LookupFailBehaviour.DEFAULT_VALUE,
				ToStringColumnMappingDefinition.OverflowBehaviour.EXCEPTION);
		assertThrows(ToStringColumnMappingDefinition.OverflowException.class,
				() -> toStringColumnMappingDefinition.convertRow(getDynamodbStringRow()));
	}

	@Test
	void testDestinationDataType() {
		final int stringLength = 10;
		final ToStringColumnMappingDefinition toStringColumnMappingDefinition = new ToStringColumnMappingDefinition(
				DEST_COLUMN, stringLength, new IdentityDynamodbResultWalker(),
				AbstractColumnMappingDefinition.LookupFailBehaviour.DEFAULT_VALUE,
				ToStringColumnMappingDefinition.OverflowBehaviour.TRUNCATE);
		assertAll(
				() -> assertThat(toStringColumnMappingDefinition.getDestinationDataType().getExaDataType(),
						equalTo(DataType.ExaDataType.VARCHAR)),
				() -> assertThat(toStringColumnMappingDefinition.getDestinationDataType().getSize(),
						equalTo(stringLength)));
	}

	@Test
	void testGetDestinationDefaultValue() {
		final ToStringColumnMappingDefinition toStringColumnMappingDefinition = new ToStringColumnMappingDefinition(
				null, 0, null, null, null);
		assertThat(toStringColumnMappingDefinition.getDestinationDefaultValue().toString(), equalTo(""));
	}

	@Test
	void testIsDestinationNullable() {
		final ToStringColumnMappingDefinition toStringColumnMappingDefinition = new ToStringColumnMappingDefinition(
				null, 0, null, null, null);
		assertThat(toStringColumnMappingDefinition.isDestinationNullable(), equalTo(true));
	}
}
