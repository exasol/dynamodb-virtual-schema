package com.exasol.adapter.document.queryplanning;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.document.mapping.MockPropertyToColumnMapping;
import com.exasol.adapter.document.mapping.TableMapping;
import com.exasol.adapter.document.querypredicate.NoPredicate;
import com.exasol.adapter.document.querypredicate.QueryPredicate;

class RemoteTableQueryTest {
    @Test
    void testSetAndGetColumns() {
        final MockPropertyToColumnMapping columnDefinition = new MockPropertyToColumnMapping("", null, null);
        final TableMapping tableDefinition = TableMapping.rootTableBuilder("", "")
                .withColumnMappingDefinition(columnDefinition).build();
        final QueryPredicate selection = new NoPredicate();
        final RemoteTableQuery remoteTableQuery = new RemoteTableQuery(tableDefinition, List.of(columnDefinition),
                selection, new NoPredicate());
        assertAll(//
                () -> assertThat(remoteTableQuery.getSelectList(), containsInAnyOrder(columnDefinition)),
                () -> assertThat(remoteTableQuery.getFromTable(), equalTo(tableDefinition)),
                () -> assertThat(remoteTableQuery.getPushDownSelection(), equalTo(selection))//
        );
    }
}
