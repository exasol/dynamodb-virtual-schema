package com.exasol.adapter.document;

import com.exasol.ExaIterator;
import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterRegistry;

/**
 * Main UDF entry point.
 */
public class ImportDocumentData {
    public static final String UDF_PREFIX = "IMPORT_FROM_";

    /**
     * This method is called by the Exasol database when the ImportFromDynamodb UDF is called.
     * 
     * @param exaMetadata exasol metadata
     * @param exaIterator iterator
     * @throws Exception if data can't get loaded
     */
    @SuppressWarnings("java:S112") // Exception is too generic. This signature is however given by the UDF framework
    public static void run(final ExaMetadata exaMetadata, final ExaIterator exaIterator) throws Exception {
        final String udfName = exaMetadata.getScriptName();
        exaMetadata.getScriptName();
        final String adapterName = udfName.replaceFirst(UDF_PREFIX, "");
        final DataLoaderUdfFactory documentAdapter = (DataLoaderUdfFactory) AdapterRegistry.getInstance()
                .getAdapterForName(adapterName);
        final DataLoaderUdf dataLoaderUdf = documentAdapter.getDataLoaderUDF();
        dataLoaderUdf.run(exaMetadata, exaIterator);
    }

    private ImportDocumentData() {
        // Intentionally empty. As this class is only accessed statical.
    }
}
