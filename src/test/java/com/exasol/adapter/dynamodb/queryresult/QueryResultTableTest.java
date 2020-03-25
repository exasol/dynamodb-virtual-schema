package com.exasol.adapter.dynamodb.queryresult;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dynamodb.mapping.ColumnMappingDefinition;
import com.exasol.adapter.metadata.DataType;
import com.exasol.cellvalue.ExasolCellValue;
import com.exasol.dynamodb.resultwalker.DynamodbResultWalker;
import com.exasol.dynamodb.resultwalker.IdentityDynamodbResultWalker;

/**
 * Tests for {@link QueryResultTable}
 */
public class QueryResultTableTest {
	@Test
	void testSetAndGetColumns() {
		final QueryResultColumn queryResultColumn = new QueryResultColumn(null);
		final QueryResultTable queryResultTable = new QueryResultTable(List.of(queryResultColumn));
		assertThat(queryResultTable.getColumns(), containsInAnyOrder(queryResultColumn));
	}

	@Test
	void testConvertRow() throws AdapterException {
		final QueryResultColumn queryResultColumn = new QueryResultColumn(new MocColumnMappingDefinition("d",
				new IdentityDynamodbResultWalker(), ColumnMappingDefinition.LookupFailBehaviour.EXCEPTION));
		final QueryResultTable queryResultTable = new QueryResultTable(List.of(queryResultColumn));
		final List<ExasolCellValue> cellValues = queryResultTable.convertRow(Map.of("isbn", new AttributeValue()));
		final List<String> literals = cellValues.stream().map(ExasolCellValue::toLiteral).collect(Collectors.toList());
		assertThat(literals, containsInAnyOrder("testValue"));
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
			return null;
		}

		@Override
		public boolean isDestinationNullable() {
			return false;
		}

		@Override
		protected ExasolCellValue convertValue(final AttributeValue dynamodbProperty) throws ColumnMappingException {
			return new ExasolCellValue() {
				@Override
				public String toLiteral() {
					return "testValue";
				}
			};
		}
	}
}
