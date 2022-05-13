package com.exasol.adapter.document.dynamodb;

import static com.exasol.adapter.document.dynamodb.DynamodbDocumentAdapterDialect.ADAPTER_NAME;

import java.util.ServiceLoader;

import com.exasol.adapter.AdapterFactory;
import com.exasol.adapter.VirtualSchemaAdapter;
import com.exasol.adapter.document.DocumentAdapter;
import com.exasol.logging.VersionCollector;

/**
 * This class implements a factory for the DynamoDB {@link DocumentAdapter}.
 * <p>
 * Note that this class must be registered in a resource file called
 * {@code META-INF/services/com.exasol.adapter.AdapterFactory} in order for the {@link ServiceLoader} to find it.
 */
public class DynamodbAdapterFactory implements AdapterFactory {

    @Override
    public VirtualSchemaAdapter createAdapter() {
        return new DocumentAdapter(new DynamodbDocumentAdapterDialect());
    }

    @Override
    public String getAdapterVersion() {
        final VersionCollector versionCollector = new VersionCollector(
                "META-INF/maven/com.exasol/virtual-schema-dynamodb/pom.properties");
        return versionCollector.getVersionNumber();
    }

    @Override
    public String getAdapterName() {
        return ADAPTER_NAME;
    }
}
