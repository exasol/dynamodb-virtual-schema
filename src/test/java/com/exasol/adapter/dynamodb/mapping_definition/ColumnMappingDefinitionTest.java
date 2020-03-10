package com.exasol.adapter.dynamodb.mapping_definition;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.mapping_definition.result_walker.IdentityDynamodbResultWalker;
import com.exasol.adapter.metadata.ColumnMetadata;

/**
 * Tests for {@link ColumnMappingDefinition}
 */
public class ColumnMappingDefinitionTest {
	@Test
	void testSerialization() throws IOException, ClassNotFoundException {
		final String name = "testName";
		final ToJsonColumnMappingDefinition expected = new ToJsonColumnMappingDefinition(name,
				new IdentityDynamodbResultWalker());
		final ColumnMetadata columnMetadata = expected.getDestinationColumn();
		final ColumnMappingDefinition actual = ColumnMappingDefinition.fromColumnMetadata(columnMetadata);
		assertThat(actual.getDestinationName(), equalTo(expected.getDestinationName()));
	}
}
