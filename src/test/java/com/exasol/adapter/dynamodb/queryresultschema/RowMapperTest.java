package com.exasol.adapter.dynamodb.queryresultschema;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbMap;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.DynamodbValueMapperFactory;
import com.exasol.adapter.dynamodb.mapping.tojsonmapping.ToJsonColumnMappingDefinition;
import com.exasol.dynamodb.attributevalue.AttributeValueQuickCreator;
import com.exasol.sql.expression.ValueExpression;

public class RowMapperTest {

    @Test
    public void testMapRow() {
        final ToJsonColumnMappingDefinition mappingDefinition = new ToJsonColumnMappingDefinition(
                new AbstractColumnMappingDefinition.ConstructorParameters("test",
                        new DocumentPathExpression.Builder().build(),
                        AbstractColumnMappingDefinition.LookupFailBehaviour.EXCEPTION));
        final QueryResultTableSchema queryResultTableSchema = new QueryResultTableSchema(null,
                List.of(mappingDefinition));
        final List<ValueExpression> exasolRow = new RowMapper<>(queryResultTableSchema,
                new DynamodbValueMapperFactory())
                        .mapRow(new DynamodbMap(Map.of("testKey", AttributeValueQuickCreator.forString("testValue"))));
        assertThat(exasolRow.get(0).toString(), equalTo("{\"testKey\":\"testValue\"}"));
    }
}
