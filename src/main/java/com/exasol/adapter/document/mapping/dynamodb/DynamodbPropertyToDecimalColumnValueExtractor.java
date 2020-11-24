package com.exasol.adapter.document.mapping.dynamodb;

import java.math.BigDecimal;

import com.exasol.adapter.document.documentnode.DocumentNode;
import com.exasol.adapter.document.documentnode.dynamodb.*;
import com.exasol.adapter.document.mapping.PropertyToDecimalColumnMapping;
import com.exasol.adapter.document.mapping.PropertyToDecimalColumnValueExtractor;

/**
 * This class converts DynamoDB Numbers to DECIMAL columns.
 */
public class DynamodbPropertyToDecimalColumnValueExtractor
        extends PropertyToDecimalColumnValueExtractor<DynamodbNodeVisitor> {
    /**
     * Create an instance of {@link DynamodbPropertyToDecimalColumnValueExtractor}.
     *
     * @param column {@link PropertyToDecimalColumnMapping} defining the extraction
     */
    public DynamodbPropertyToDecimalColumnValueExtractor(final PropertyToDecimalColumnMapping column) {
        super(column);
    }

    @Override
    protected ConversionResult mapValueToDecimal(final DocumentNode<DynamodbNodeVisitor> documentValue) {
        final ConversionVisitor visitor = new ConversionVisitor();
        documentValue.accept(visitor);
        return visitor.getResult();
    }

    private static class ConversionVisitor implements DynamodbNodeVisitor {
        private ConversionResult result;

        @Override
        public void visit(final DynamodbString string) {
            this.result = new NotNumericResult(string.getValue());
        }

        @Override
        public void visit(final DynamodbNumber number) {
            this.result = new ConvertedResult(new BigDecimal(number.getValue()));
        }

        @Override
        public void visit(final DynamodbBinary binary) {
            this.result = new NotNumericResult("<binary>");
        }

        @Override
        public void visit(final DynamodbBoolean bool) {
            this.result = new NotNumericResult("<" + (bool.getValue() ? "true" : "false") + ">");
        }

        @Override
        public void visit(final DynamodbStringSet stringSet) {
            this.result = new NotNumericResult("<string set>");
        }

        @Override
        public void visit(final DynamodbBinarySet binarySet) {
            this.result = new NotNumericResult("<binary set>");
        }

        @Override
        public void visit(final DynamodbNumberSet numberSet) {
            this.result = new NotNumericResult("<number set>");
        }

        @Override
        public void visit(final DynamodbList list) {
            this.result = new NotNumericResult("<list>");
        }

        @Override
        public void visit(final DynamodbMap map) {
            this.result = new NotNumericResult("<map>");
        }

        @Override
        public void visit(final DynamodbNull nullValue) {
            this.result = new NotNumericResult("<null>");
        }

        /**
         * Get the result of the conversion.
         * 
         * @return result of the conversion
         */
        public ConversionResult getResult() {
            return this.result;
        }
    }
}
