package com.exasol.adapter.document.mapping;

import com.exasol.adapter.document.documentpath.DocumentPathExpression;

public class PropertyToColumnMappingBuilderQuickAccess {
    public static final String DEFAULT_EXASOL_COLUMN_NAME = "EXASOL_COLUMN";
    public static final MappingErrorBehaviour DEFAULT_LOOKUP_FAIL_BEHAVIOUR = MappingErrorBehaviour.ABORT;
    public static final DocumentPathExpression DEFAULT_PATH = DocumentPathExpression.builder().addObjectLookup("key")
            .build();

    public static <T extends PropertyToColumnMapping.Builder> T configureExampleMapping(final T builder) {
        builder.exasolColumnName(DEFAULT_EXASOL_COLUMN_NAME)//
                .lookupFailBehaviour(DEFAULT_LOOKUP_FAIL_BEHAVIOUR)//
                .pathToSourceProperty(DEFAULT_PATH);
        return builder;
    }

    public static PropertyToColumnMapping.Builder getColumnMappingExample() {
        return PropertyToJsonColumnMapping.builder().exasolColumnName("test")
                .pathToSourceProperty(DocumentPathExpression.empty()).varcharColumnSize(10)
                .lookupFailBehaviour(MappingErrorBehaviour.ABORT).overflowBehaviour(MappingErrorBehaviour.ABORT);
    }
}
