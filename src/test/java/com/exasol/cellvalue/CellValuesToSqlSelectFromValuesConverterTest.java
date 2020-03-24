package com.exasol.cellvalue;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.mapping.ToJsonColumnMappingDefinition;
import com.exasol.adapter.dynamodb.queryresult.QueryResultColumn;
import com.exasol.adapter.dynamodb.queryresult.QueryResultTable;
import com.exasol.dynamodb.resultwalker.IdentityDynamodbResultWalker;

/**
 * Tests for {@link CellValuesToSqlSelectFromValuesConverter}.
 */
public class CellValuesToSqlSelectFromValuesConverterTest {
	QueryResultTable getTestTable() {
		return new QueryResultTable(List.of(
				new QueryResultColumn(new ToJsonColumnMappingDefinition("json", new IdentityDynamodbResultWalker()))));
	}

	@Test
	public void testEmptyConvert() {
		final CellValuesToSqlSelectFromValuesConverter converter = new CellValuesToSqlSelectFromValuesConverter();
		final String sql = converter.convert(getTestTable(), Collections.emptyList());
		assertThat(sql, equalTo("SELECT * FROM VALUES(NULL) WHERE 0 = 1;"));
	}

	@Test
	public void testSingleItemConvert() {
		final String testString = "test";
		final ExasolCellValue stringFrame = new StringExasolCellValue(testString);
		final CellValuesToSqlSelectFromValuesConverter converter = new CellValuesToSqlSelectFromValuesConverter();
		final String sql = converter.convert(getTestTable(), List.of(List.of(stringFrame)));
		assertThat(sql, equalTo("SELECT * FROM (VALUES('" + testString + "'));"));
	}

	@Test
	public void testTwoItemConvert() {
		final String testString1 = "test1";
		final ExasolCellValue stringFrame1 = new StringExasolCellValue(testString1);
		final String testString2 = "test2";
		final ExasolCellValue stringFrame2 = new StringExasolCellValue(testString2);

		final CellValuesToSqlSelectFromValuesConverter converter = new CellValuesToSqlSelectFromValuesConverter();
		final String sql = converter.convert(getTestTable(), List.of(List.of(stringFrame1), List.of(stringFrame2)));
		assertThat(sql, equalTo("SELECT * FROM (VALUES('" + testString1 + "'), ('" + testString2 + "'));"));
	}
}
