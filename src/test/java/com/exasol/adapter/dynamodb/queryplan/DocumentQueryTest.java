package com.exasol.adapter.dynamodb.queryplan;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.mapping.MockColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.TableMappingDefinition;

public class DocumentQueryTest {
    @Test
    void testSetAndGetColumns() {
        final MockColumnMappingDefinition columnDefinition = new MockColumnMappingDefinition("", null, null);
        final TableMappingDefinition tableDefinition = TableMappingDefinition.rootTableBuilder("", "")
                .withColumnMappingDefinition(columnDefinition).build();
        final DocumentQuery documentQuery = new DocumentQuery(tableDefinition, List.of(columnDefinition));
        assertAll(() -> assertThat(documentQuery.getSelectList(), containsInAnyOrder(columnDefinition)),
                () -> assertThat(documentQuery.getFromTable(), equalTo(tableDefinition)));
    }
}
