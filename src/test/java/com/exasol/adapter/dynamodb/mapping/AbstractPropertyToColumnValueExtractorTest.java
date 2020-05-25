package com.exasol.adapter.dynamodb.mapping;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentnode.DocumentObject;
import com.exasol.adapter.dynamodb.documentnode.DocumentValue;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathWalkerException;
import com.exasol.adapter.dynamodb.documentpath.StaticDocumentPathIterator;
import com.exasol.sql.expression.ValueExpression;

class AbstractPropertyToColumnValueExtractorTest {
    @Test
    void testLookup() {
        final DocumentPathExpression sourcePath = new DocumentPathExpression.Builder().addObjectLookup("isbn").build();
        final MockPropertyToColumnMapping columnMappingDefinition = new MockPropertyToColumnMapping("d", sourcePath,
                LookupFailBehaviour.EXCEPTION);

        final ValueMapperStub valueMapperStub = new ValueMapperStub(columnMappingDefinition);
        final StubDocumentObject testObject = new StubDocumentObject();
        valueMapperStub.extractColumnValue(testObject, new StaticDocumentPathIterator());
        assertThat(valueMapperStub.remoteValue, equalTo(StubDocumentObject.MAP.get("isbn")));
    }

    @Test
    void testNullLookupFailBehaviour() throws ColumnValueExtractorException {
        final DocumentPathExpression sourcePath = new DocumentPathExpression.Builder()
                .addObjectLookup("nonExistingColumn").build();
        final MockPropertyToColumnMapping columnMappingDefinition = new MockPropertyToColumnMapping("d", sourcePath,
                LookupFailBehaviour.DEFAULT_VALUE);
        final ValueExpression valueExpression = new ValueMapperStub(columnMappingDefinition)
                .extractColumnValue(new StubDocumentObject(), new StaticDocumentPathIterator());
        assertThat(valueExpression.toString(), equalTo("default"));
    }

    @Test
    void testExceptionLookupFailBehaviour() {
        final DocumentPathExpression sourcePath = new DocumentPathExpression.Builder()
                .addObjectLookup("nonExistingColumn").build();
        final MockPropertyToColumnMapping columnMappingDefinition = new MockPropertyToColumnMapping("d", sourcePath,
                LookupFailBehaviour.EXCEPTION);
        assertThrows(DocumentPathWalkerException.class, () -> new ValueMapperStub(columnMappingDefinition)
                .extractColumnValue(new StubDocumentObject(), new StaticDocumentPathIterator()));
    }

    @Test
    void testColumnMappingException() {
        final String columnName = "name";
        final MockPropertyToColumnMapping mappingDefinition = new MockPropertyToColumnMapping(columnName,
                DocumentPathExpression.empty(), LookupFailBehaviour.EXCEPTION);
        final ColumnValueExtractorException exception = assertThrows(ColumnValueExtractorException.class,
                () -> new ExceptionMockColumnValueMapper(mappingDefinition).extractColumnValue(new StubDocumentObject(),
                        new StaticDocumentPathIterator()));
        assertThat(exception.getCausingColumn().getExasolColumnName(), equalTo(columnName));
    }

    private static class DummyVisitor {

    }

    private static class StubDocumentObject implements DocumentObject<DummyVisitor> {
        private static final Map<String, DocumentNode<DummyVisitor>> MAP = Map.of("isbn", new StubDocumentValue());

        @Override
        public Map<String, DocumentNode<DummyVisitor>> getKeyValueMap() {
            return MAP;
        }

        @Override
        public DocumentNode<DummyVisitor> get(final String key) {
            return MAP.get(key);
        }

        @Override
        public boolean hasKey(final String key) {
            return MAP.containsKey(key);
        }

        @Override
        public void accept(final DummyVisitor visitor) {

        }
    }

    private static class StubDocumentValue implements DocumentValue<DummyVisitor> {

        @Override
        public void accept(final DummyVisitor visitor) {

        }
    }

    private static class ValueMapperStub extends AbstractPropertyToColumnValueExtractor<DummyVisitor> {
        private DocumentNode<DummyVisitor> remoteValue;

        public ValueMapperStub(final PropertyToColumnMapping column) {
            super(column);
        }

        @Override
        protected ValueExpression mapValue(final DocumentNode<DummyVisitor> documentValue) {
            this.remoteValue = documentValue;
            return null;
        }
    }

    private static class ExceptionMockColumnValueMapper extends AbstractPropertyToColumnValueExtractor<DummyVisitor> {
        private final ColumnMapping column;

        public ExceptionMockColumnValueMapper(final PropertyToColumnMapping column) {
            super(column);
            this.column = column;
        }

        @Override
        protected ValueExpression mapValue(final DocumentNode<DummyVisitor> documentValue) {
            throw new ColumnValueExtractorException("mocMessage", this.column);
        }
    }
}
