package com.exasol.adapter.dynamodb.queryresult;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.List;

import org.junit.jupiter.api.Test;

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
}
