package com.exasol.adapter.dynamodb;

import com.amazonaws.services.dynamodbv2.document.Item;

/**
 * This implementation of {@link DynamodbResultToSqlConverter} converts a
 * DynamoDB result into an {@code SELECT FROM VALUES} statement.
 */
public class DynamodbResultToSqlSelectFromValuesConverter implements DynamodbResultToSqlConverter {
	@Override
	public String convert(final Iterable<Item> items) {
		if (!items.iterator().hasNext()) {
			return "SELECT * FROM VALUES('') WHERE 0 = 1;";
		}
		final StringBuilder responseBuilder = new StringBuilder("SELECT * FROM (VALUES");
		boolean isFirst = true;
		for (final Item item : items) {
			if (!isFirst) {
				responseBuilder.append(", ");
			}
			isFirst = false;
			responseBuilder.append("('").append(item.getString("isbn")).append("')");
		}
		responseBuilder.append(");");
		return responseBuilder.toString();
	}
}
