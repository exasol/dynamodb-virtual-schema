package com.exasol.adapter.dynamodb.queryresultschema;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.tojsonmapping.ToJsonColumnMappingDefinition;
import com.exasol.dynamodb.attributevalue.AttributeValueQuickCreator;
import com.exasol.dynamodb.resultwalker.IdentityDynamodbResultWalker;
import com.exasol.sql.expression.ValueExpression;

public class RowMapperTest {

    @Test
    public void testMapRow() {
        final ToJsonColumnMappingDefinition mappingDefinition = new ToJsonColumnMappingDefinition(
                new AbstractColumnMappingDefinition.ConstructorParameters("test", new IdentityDynamodbResultWalker(),
                        AbstractColumnMappingDefinition.LookupFailBehaviour.EXCEPTION));
        final QueryResultTableSchema queryResultTableSchema = new QueryResultTableSchema(null,
                List.of(mappingDefinition));
        final List<ValueExpression> exasolRow = new RowMapper(queryResultTableSchema)
                .mapRow(Map.of("testKey", AttributeValueQuickCreator.forString("testValue")));
        assertThat(exasolRow.get(0).toString(), equalTo("{\"testKey\":\"testValue\"}"));
    }
}
