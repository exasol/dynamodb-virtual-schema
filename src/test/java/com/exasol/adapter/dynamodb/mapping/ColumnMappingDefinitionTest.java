package com.exasol.adapter.dynamodb.mapping;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.metadata.DataType;
import com.exasol.cellvalue.ExasolCellValue;
import com.exasol.dynamodb.resultwalker.DynamodbResultWalker;
import com.exasol.dynamodb.resultwalker.ObjectDynamodbResultWalker;

public class ColumnMappingDefinitionTest {
	@Test
	void testDestinationName() {
		final String destinationName = "destinationName";
		final MocColumnMappingDefinition columnMappingDefinition = new MocColumnMappingDefinition(destinationName, null,
				null);
		assertThat(columnMappingDefinition.getDestinationName(), equalTo(destinationName));
	}

	@Test
	void testLookup()
			throws DynamodbResultWalker.DynamodbResultWalkerException, ColumnMappingDefinition.ColumnMappingException {
		final ObjectDynamodbResultWalker resultWalker = new ObjectDynamodbResultWalker("isbn", null);
		final MocColumnMappingDefinition columnMappingDefinition = new MocColumnMappingDefinition("d", resultWalker,
				ColumnMappingDefinition.LookupFailBehaviour.EXCEPTION);
		final String isbn = "123456789";
		final AttributeValue isbnValue = new AttributeValue();
		isbnValue.setS(isbn);
		final ExasolCellValue exasolCellValue = columnMappingDefinition.convertRow(Map.of("isbn", isbnValue));
		assertThat(exasolCellValue.toLiteral(), equalTo(isbn));
	}

	@Test
	void testNullLookupFailBehaviour()
			throws ColumnMappingDefinition.ColumnMappingException, DynamodbResultWalker.DynamodbResultWalkerException {
		final ObjectDynamodbResultWalker resultWalker = new ObjectDynamodbResultWalker("nonExistingColumn", null);
		final MocColumnMappingDefinition columnMappingDefinition = new MocColumnMappingDefinition("d", resultWalker,
				ColumnMappingDefinition.LookupFailBehaviour.DEFAULT_VALUE);
		final ExasolCellValue exasolCellValue = columnMappingDefinition.convertRow(Map.of());
		assertThat(exasolCellValue.toLiteral(), equalTo("default"));
	}

	@Test
	void testExceptionLookupFailBehaviour() {
		final ObjectDynamodbResultWalker resultWalker = new ObjectDynamodbResultWalker("nonExistingColumn", null);
		final MocColumnMappingDefinition columnMappingDefinition = new MocColumnMappingDefinition("d", resultWalker,
				ColumnMappingDefinition.LookupFailBehaviour.EXCEPTION);
		assertThrows(DynamodbResultWalker.DynamodbResultWalkerException.class,
				() -> columnMappingDefinition.convertRow(Map.of()));
	}

	private static class MocExasolCellValue implements ExasolCellValue {
		private final String value;

		private MocExasolCellValue(final String value) {
			this.value = value;
		}

		@Override
		public String toLiteral() {
			return this.value;
		}
	}

	private static class MocColumnMappingDefinition extends ColumnMappingDefinition {
		public MocColumnMappingDefinition(final String destinationName, final DynamodbResultWalker resultWalker,
				final LookupFailBehaviour lookupFailBehaviour) {
			super(destinationName, resultWalker, lookupFailBehaviour);
		}

		@Override
		public DataType getDestinationDataType() {
			return null;
		}

		@Override
		public ExasolCellValue getDestinationDefaultValue() {
			return new MocExasolCellValue("default");
		}

		@Override
		public boolean isDestinationNullable() {
			return false;
		}

		/**
		 * Converts the attribute value to its string value.
		 *
		 * @param dynamodbProperty
		 * @return
		 */
		@Override
		protected ExasolCellValue convertValue(final AttributeValue dynamodbProperty) {
			return new MocExasolCellValue(dynamodbProperty.getS());
		}
	}
}
