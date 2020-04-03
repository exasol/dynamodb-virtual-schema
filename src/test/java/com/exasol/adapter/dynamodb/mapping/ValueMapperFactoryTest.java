package com.exasol.adapter.dynamodb.mapping;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.mapping.tojsonmapping.ToJsonColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.tojsonmapping.ToJsonValueMapper;
import com.exasol.adapter.dynamodb.mapping.tostringmapping.ToStringColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.tostringmapping.ToStringValueMapper;

public class ValueMapperFactoryTest {

	@Test
	void testToStringMapping() {
		final ToStringColumnMappingDefinition mappingDefinition = new ToStringColumnMappingDefinition("", 10, null,
				null, null);
		final AbstractValueMapper valueMapper = new ValueMapperFactory().getValueMapperForColumn(mappingDefinition);
		assertThat(valueMapper.getClass(), equalTo(ToStringValueMapper.class));
	}

	@Test
	void testToJsonMapping() {
		final ToJsonColumnMappingDefinition mappingDefinition = new ToJsonColumnMappingDefinition("", null, null);
		final AbstractValueMapper valueMapper = new ValueMapperFactory().getValueMapperForColumn(mappingDefinition);
		assertThat(valueMapper.getClass(), equalTo(ToJsonValueMapper.class));
	}
}
