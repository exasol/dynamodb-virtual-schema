package com.exasol.adapter.dynamodb.queryresultschema;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.mapping.MockColumnMappingDefinition;

public class QueryResultTableSchemaTest {
    @Test
    void testSetAndGetColumns() {
        final MockColumnMappingDefinition columnDefinition = new MockColumnMappingDefinition("", null, null);
        final QueryResultTableSchema queryResultTableSchema = new QueryResultTableSchema(List.of(columnDefinition));
        assertThat(queryResultTableSchema.getColumns(), containsInAnyOrder(columnDefinition));
    }
}
