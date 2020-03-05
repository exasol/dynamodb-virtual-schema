package com.exasol.adapter.dynamodb;

import java.util.List;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;

/**
 * Interface for converting a DynamoDB response into a SQL Statement for
 * pushdown response.
 */
public abstract class DynamodbResultToSqlConverter {
	/**
	 * Converts the DynamoDB result into an SQL Statement.
	 * 
	 * @param items
	 *            for example: {@code ItemCollection<ScanOutcome> }
	 * @return SQL Statement
	 */
	public abstract String convert(final Iterable<Item> items);

	/**
	 * Converts a single Item result into an SQL Statement.
	 * 
	 * @param item
	 *            for example the result of
	 *            {@link com.amazonaws.services.dynamodbv2.document.Table#getItem(GetItemSpec)}
	 * @return SQL Statement
	 */
	public String convert(final Item item) {
		return this.convert(List.of(item));
	}
}
