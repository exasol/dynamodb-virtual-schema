package com.exasol.adapter.dynamodb.exasol_dataframe;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.jupiter.api.Test;

public class StringExasolDataFrameTest {

	void testToLiteral(final String testString, final String expected) {
		final StringExasolDataFrame exasolDataFrame = new StringExasolDataFrame(testString);
		assertThat(exasolDataFrame.toLiteral(), equalTo(expected));
	}

	@Test
	void testNormalStringToLiteral() {
		testToLiteral("test", "'test'");
	}

	@Test
	void testEmptyStringToLiteral() {
		testToLiteral("", "''");
	}

	@Test
	void testInjection1ToLiteral() {
		testToLiteral("'", "'\\''");
	}

	@Test
	void testInjection2ToLiteral() {
		testToLiteral("\\'", "'\\\\''");
	}

	@Test
	void testNullStringToLiteral() {
		testToLiteral(null, "NULL");
	}
}
