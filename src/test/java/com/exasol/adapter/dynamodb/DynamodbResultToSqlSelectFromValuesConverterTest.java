package com.exasol.adapter.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.document.Item;

/**
 * Tests for {@link DynamodbResultToSqlSelectFromValuesConverter}.
 */
public class DynamodbResultToSqlSelectFromValuesConverterTest {
	@Test
	public void testEmptyConvert() {
		final DynamodbResultToSqlSelectFromValuesConverter converter = new DynamodbResultToSqlSelectFromValuesConverter();
		final String sql = converter.convert(Collections.emptyList());
		assertThat(sql, equalTo("SELECT * FROM VALUES('') WHERE 0 = 1;"));
	}

	@Test
	public void testSingleItemConvert() {
		final Item testItem = Item.fromJSON("{\"isbn\":\"1\"}");
		final DynamodbResultToSqlSelectFromValuesConverter converter = new DynamodbResultToSqlSelectFromValuesConverter();
		final String sql = converter.convert(List.of(testItem));
		assertThat(sql, equalTo("SELECT * FROM (VALUES('1'));"));
	}

	@Test
	public void testTwoItemConvert() {
		final Item testItem1 = Item.fromJSON("{\"isbn\":\"1\"}");
		final Item testItem2 = Item.fromJSON("{\"isbn\":\"2\"}");
		final DynamodbResultToSqlSelectFromValuesConverter converter = new DynamodbResultToSqlSelectFromValuesConverter();
		final String sql = converter.convert(List.of(testItem1, testItem2));
		assertThat(sql, equalTo("SELECT * FROM (VALUES('1'), ('2'));"));
	}
}
