package com.exasol.adapter.document.mapping;

import com.exasol.adapter.document.documentpath.DocumentPathExpression;
import com.exasol.adapter.metadata.DataType;

public class MockPropertyToColumnMapping extends AbstractPropertyToColumnMapping {
    private static final long serialVersionUID = 190719191206210825L;//

    public MockPropertyToColumnMapping(final String destinationName, final DocumentPathExpression sourcePath,
            final MappingErrorBehaviour lookupFailBehaviour) {
        super(destinationName, sourcePath, lookupFailBehaviour);
    }

    @Override
    public DataType getExasolDataType() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public boolean isExasolColumnNullable() {
        return false;
    }

    @Override
    public void accept(final PropertyToColumnMappingVisitor visitor) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public ColumnMapping withNewExasolName(final String newExasolName) {
        return new MockPropertyToColumnMapping(newExasolName, getPathToSourceProperty(), getLookupFailBehaviour());
    }
}
