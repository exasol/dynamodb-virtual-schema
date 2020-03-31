package com.exasol.adapter.dynamodb.mapping;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.AdapterException;
import com.exasol.dynamodb.attributevalue.AttributeValueTestUtils;
import com.exasol.dynamodb.resultwalker.IdentityDynamodbResultWalker;
import com.exasol.sql.expression.ValueExpression;

/**
 * Test for {@link ToJsonColumnMappingDefinition}
 */
public class ToJsonColumnMappingDefinitionTest {

	private static final String DEST_COLUMN = "destColumn";

	@Test
	void testConvertRowBasic() throws AdapterException {
		final ToJsonColumnMappingDefinition toStringColumnMappingDefinition = new ToJsonColumnMappingDefinition(
				DEST_COLUMN, new IdentityDynamodbResultWalker(),
				AbstractColumnMappingDefinition.LookupFailBehaviour.EXCEPTION);
		final Map<String, AttributeValue> rootAttributeValue = Map.of("key",
				AttributeValueTestUtils.forString("value"));
		final ValueExpression exasolCellValue = toStringColumnMappingDefinition.convertRow(rootAttributeValue);
		assertThat(exasolCellValue.toString(), equalTo("{\"key\":\"value\"}"));
	}
}
