package com.exasol.adapter.dynamodb.remotetablequery;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbMap;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.RowMapper;
import com.exasol.adapter.dynamodb.mapping.ToJsonColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.dynamodb.DynamodbValueMapperFactory;
import com.exasol.dynamodb.attributevalue.AttributeValueQuickCreator;
import com.exasol.sql.expression.ValueExpression;

public class RowMapperTest {
    // TODO remove DynamoDB dependency
    @Test
    public void testMapRow() {
        final ToJsonColumnMappingDefinition mappingDefinition = new ToJsonColumnMappingDefinition(
                new AbstractColumnMappingDefinition.ConstructorParameters("test",
                        new DocumentPathExpression.Builder().build(),
                        AbstractColumnMappingDefinition.LookupFailBehaviour.EXCEPTION));
        final RemoteTableQuery<DynamodbNodeVisitor> remoteTableQuery = new RemoteTableQuery<>(null,
                List.of(mappingDefinition), new NoPredicate<>());
        final List<ValueExpression> exasolRow = new RowMapper<>(remoteTableQuery, new DynamodbValueMapperFactory())
                .mapRow(new DynamodbMap(Map.of("testKey", AttributeValueQuickCreator.forString("testValue"))));
        assertThat(exasolRow.get(0).toString(), equalTo("{\"testKey\":\"testValue\"}"));
    }
}
