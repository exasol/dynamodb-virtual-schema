package com.exasol.adapter.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.exasol_dataframe.ExasolDataFrame;
import com.exasol.adapter.dynamodb.exasol_dataframe.StringExasolDataFrame;
import com.exasol.adapter.dynamodb.mapping_definition.ToJsonColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping_definition.result_walker.IdentityDynamodbResultWalker;

/**
 * Tests for {@link DynamodbResultToSqlSelectFromValuesConverter}.
 */
public class DynamodbResultToSqlSelectFromValuesConverterTest {
	QueryResultTable getTestTable() {
		return new QueryResultTable(List.of(
				new QueryResultColumn(new ToJsonColumnMappingDefinition("json", new IdentityDynamodbResultWalker()))));
	}

	@Test
	public void testEmptyConvert() {
		final DynamodbResultToSqlSelectFromValuesConverter converter = new DynamodbResultToSqlSelectFromValuesConverter();
		final String sql = converter.convert(getTestTable(), Collections.emptyList());
		assertThat(sql, equalTo("SELECT * FROM VALUES(NULL) WHERE 0 = 1;"));
	}

	@Test
	public void testSingleItemConvert() {
		final String testString = "test";
		final ExasolDataFrame stringFrame = new StringExasolDataFrame(testString);
		final DynamodbResultToSqlSelectFromValuesConverter converter = new DynamodbResultToSqlSelectFromValuesConverter();
		final String sql = converter.convert(getTestTable(), List.of(List.of(stringFrame)));
		assertThat(sql, equalTo("SELECT * FROM (VALUES('" + testString + "'));"));
	}

	@Test
	public void testTwoItemConvert() {
		final String testString1 = "test1";
		final ExasolDataFrame stringFrame1 = new StringExasolDataFrame(testString1);
		final String testString2 = "test2";
		final ExasolDataFrame stringFrame2 = new StringExasolDataFrame(testString2);

		final DynamodbResultToSqlSelectFromValuesConverter converter = new DynamodbResultToSqlSelectFromValuesConverter();
		final String sql = converter.convert(getTestTable(), List.of(List.of(stringFrame1), List.of(stringFrame2)));
		assertThat(sql, equalTo("SELECT * FROM (VALUES('" + testString1 + "'), ('" + testString2 + "'));"));
	}
}
