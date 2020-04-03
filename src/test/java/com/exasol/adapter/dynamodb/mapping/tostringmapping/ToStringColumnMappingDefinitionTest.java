package com.exasol.adapter.dynamodb.mapping.tostringmapping;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.adapter.metadata.DataType;
import com.exasol.dynamodb.resultwalker.IdentityDynamodbResultWalker;

/**
 * Tests for {@link ToStringColumnMappingDefinition}
 */
public class ToStringColumnMappingDefinitionTest {
	private static final String DEST_COLUMN = "destColumn";

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
