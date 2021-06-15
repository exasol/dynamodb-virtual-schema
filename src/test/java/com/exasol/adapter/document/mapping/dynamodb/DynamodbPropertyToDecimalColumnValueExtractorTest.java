package com.exasol.adapter.document.mapping.dynamodb;

import static com.exasol.adapter.document.mapping.PropertyToColumnMappingBuilderQuickAccess.configureExampleMapping;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.exasol.adapter.document.documentnode.DocumentNode;
import com.exasol.adapter.document.documentnode.dynamodb.*;
import com.exasol.adapter.document.mapping.MappingErrorBehaviour;
import com.exasol.adapter.document.mapping.PropertyToDecimalColumnMapping;
import com.exasol.adapter.document.mapping.PropertyToDecimalColumnValueExtractor.ConvertedResult;
import com.exasol.adapter.document.mapping.PropertyToDecimalColumnValueExtractor.NotNumericResult;

import software.amazon.awssdk.core.SdkBytes;

class DynamodbPropertyToDecimalColumnValueExtractorTest {

    public static final PropertyToDecimalColumnMapping MAPPING = configureExampleMapping(
            PropertyToDecimalColumnMapping.builder())//
                    .lookupFailBehaviour(MappingErrorBehaviour.ABORT)//
                    .decimalPrecision(10)//
                    .decimalScale(2)//
                    .overflowBehaviour(MappingErrorBehaviour.ABORT)//
                    .notNumericBehaviour(MappingErrorBehaviour.ABORT)//
                    .build();
    public static final DynamodbPropertyToDecimalColumnValueExtractor EXTRACTOR = new DynamodbPropertyToDecimalColumnValueExtractor(
            MAPPING);

    private static Stream<Arguments> expectedNoNumericMapping() {
        return Stream.of(//
                Arguments.of(new DynamodbString("test"), "test"), //
                Arguments.of(new DynamodbList(List.of()), "<list>"), //
                Arguments.of(new DynamodbStringSet(List.of()), "<string set>"), //
                Arguments.of(new DynamodbNumberSet(List.of()), "<number set>"), //
                Arguments.of(new DynamodbBinarySet(List.of()), "<binary set>"), //
                Arguments.of(new DynamodbBoolean(true), "<true>"), //
                Arguments.of(new DynamodbBoolean(false), "<false>"), //
                Arguments.of(new DynamodbNull(), "<null>"), //
                Arguments.of(new DynamodbBinary(SdkBytes.fromByteArray("".getBytes())), "<binary>"), //
                Arguments.of(new DynamodbMap(Map.of()), "<map>")//
        );
    }

    @Test
    void testDecimal() {
        final ConvertedResult result = (ConvertedResult) EXTRACTOR.mapValueToDecimal(new DynamodbNumber("1234.45"));
        assertThat(result.getResult(), equalTo(BigDecimal.valueOf(1234.45)));
    }

    @ParameterizedTest
    @MethodSource("expectedNoNumericMapping")
    void testNotNumerics(final DocumentNode<DynamodbNodeVisitor> input, final String expectedValueDescription) {
        final NotNumericResult result = (NotNumericResult) EXTRACTOR.mapValueToDecimal(input);
        assertThat(result.getValue(), equalTo(expectedValueDescription));
    }
}