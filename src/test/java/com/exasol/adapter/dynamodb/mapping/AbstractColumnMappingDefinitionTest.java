package com.exasol.adapter.dynamodb.mapping;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.metadata.DataType;
import com.exasol.dynamodb.resultwalker.AbstractDynamodbResultWalker;
import com.exasol.dynamodb.resultwalker.IdentityDynamodbResultWalker;
import com.exasol.dynamodb.resultwalker.ObjectDynamodbResultWalker;
import com.exasol.sql.expression.StringLiteral;
import com.exasol.sql.expression.ValueExpression;

public class AbstractColumnMappingDefinitionTest {
	@Test
	void testDestinationName() {
		final String destinationName = "destinationName";
		final MocColumnMappingDefinition columnMappingDefinition = new MocColumnMappingDefinition(destinationName, null,
				null);
		assertThat(columnMappingDefinition.getDestinationName(), equalTo(destinationName));
	}

	@Test
	void testLookup() throws AbstractDynamodbResultWalker.DynamodbResultWalkerException,
			AbstractColumnMappingDefinition.ColumnMappingException {
		final ObjectDynamodbResultWalker resultWalker = new ObjectDynamodbResultWalker("isbn", null);
		final MocColumnMappingDefinition columnMappingDefinition = new MocColumnMappingDefinition("d", resultWalker,
				AbstractColumnMappingDefinition.LookupFailBehaviour.EXCEPTION);
		final String isbn = "123456789";
		final AttributeValue isbnValue = new AttributeValue();
		isbnValue.setS(isbn);
		final ValueExpression valueExpression = columnMappingDefinition.convertRow(Map.of("isbn", isbnValue));
		assertThat(valueExpression.toString(), equalTo(isbn));
	}

	@Test
	void testGetDestinationDefaultValueLiteral() {
		final String destinationName = "destinationName";
		final MocColumnMappingDefinition columnMappingDefinition = new MocColumnMappingDefinition(destinationName, null,
				null);
		assertThat(columnMappingDefinition.getDestinationDefaultValueLiteral(), equalTo("'default'"));
	}

	@Test
	void testNullLookupFailBehaviour() throws AbstractColumnMappingDefinition.ColumnMappingException,
			AbstractDynamodbResultWalker.DynamodbResultWalkerException {
		final ObjectDynamodbResultWalker resultWalker = new ObjectDynamodbResultWalker("nonExistingColumn", null);
		final MocColumnMappingDefinition columnMappingDefinition = new MocColumnMappingDefinition("d", resultWalker,
				AbstractColumnMappingDefinition.LookupFailBehaviour.DEFAULT_VALUE);
		final ValueExpression valueExpression = columnMappingDefinition.convertRow(Map.of());
		assertThat(valueExpression.toString(), equalTo("default"));
	}

	@Test
	void testExceptionLookupFailBehaviour() {
		final ObjectDynamodbResultWalker resultWalker = new ObjectDynamodbResultWalker("nonExistingColumn", null);
		final MocColumnMappingDefinition columnMappingDefinition = new MocColumnMappingDefinition("d", resultWalker,
				AbstractColumnMappingDefinition.LookupFailBehaviour.EXCEPTION);
		assertThrows(AbstractDynamodbResultWalker.DynamodbResultWalkerException.class,
				() -> columnMappingDefinition.convertRow(Map.of()));
	}

	@Test
	public void testColumnMappingException() {
		final String columnName = "name";
		final ExceptionMocColumnMappingDefinition mappingDefinition = new ExceptionMocColumnMappingDefinition(
				columnName, new IdentityDynamodbResultWalker(),
				AbstractColumnMappingDefinition.LookupFailBehaviour.EXCEPTION);
		final AbstractColumnMappingDefinition.ColumnMappingException exception = assertThrows(
				AbstractColumnMappingDefinition.ColumnMappingException.class,
				() -> mappingDefinition.convertRow(Map.of()));
		assertThat(exception.getCausingColumn().getDestinationName(), equalTo(columnName));
	}

	private static class MocColumnMappingDefinition extends AbstractColumnMappingDefinition {
		public MocColumnMappingDefinition(final String destinationName, final AbstractDynamodbResultWalker resultWalker,
				final LookupFailBehaviour lookupFailBehaviour) {
			super(destinationName, resultWalker, lookupFailBehaviour);
		}

		@Override
		public DataType getDestinationDataType() {
			return null;
		}

		@Override
		public ValueExpression getDestinationDefaultValue() {
			return StringLiteral.of("default");
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
		protected ValueExpression convertValue(final AttributeValue dynamodbProperty) {
			return StringLiteral.of(dynamodbProperty.getS());
		}
	}

	private static class ExceptionMocColumnMappingDefinition extends AbstractColumnMappingDefinition {
		public ExceptionMocColumnMappingDefinition(final String destinationName,
				final AbstractDynamodbResultWalker resultWalker, final LookupFailBehaviour lookupFailBehaviour) {
			super(destinationName, resultWalker, lookupFailBehaviour);
		}

		@Override
		public DataType getDestinationDataType() {
			return null;
		}

		@Override
		public ValueExpression getDestinationDefaultValue() {
			return null;
		}

		@Override
		public boolean isDestinationNullable() {
			return false;
		}

		@Override
		protected ValueExpression convertValue(final AttributeValue dynamodbProperty) throws ColumnMappingException {
			throw new ColumnMappingException("message", this);
		}
	}
}
