package com.exasol.adapter.dynamodb.remotetablequery;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.mapping.MockPropertyToColumnMapping;
import com.exasol.adapter.dynamodb.mapping.TableMapping;

public class RemoteTableQueryTest {
    @Test
    void testSetAndGetColumns() {
        final MockPropertyToColumnMapping columnDefinition = new MockPropertyToColumnMapping("", null, null);
        final TableMapping tableDefinition = TableMapping.rootTableBuilder("", "")
                .withColumnMappingDefinition(columnDefinition).build();
        final QueryPredicate<Object> selection = new NoPredicate<>();
        final RemoteTableQuery<Object> remoteTableQuery = new RemoteTableQuery<>(tableDefinition,
                List.of(columnDefinition), selection);
        assertAll(//
                () -> assertThat(remoteTableQuery.getSelectList(), containsInAnyOrder(columnDefinition)),
                () -> assertThat(remoteTableQuery.getFromTable(), equalTo(tableDefinition)),
                () -> assertThat(remoteTableQuery.getSelection(), equalTo(selection))//
        );
    }
}
