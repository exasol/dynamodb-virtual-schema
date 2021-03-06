package com.exasol.adapter.document.dynamodb;

import static com.exasol.adapter.document.dynamodb.DynamodbAdapter.ADAPTER_NAME;

import java.util.ServiceLoader;
import java.util.Set;

import com.exasol.adapter.AdapterFactory;
import com.exasol.adapter.VirtualSchemaAdapter;
import com.exasol.logging.VersionCollector;

/**
 * This class implements a factory for the {@link DynamodbAdapter}.
 * <p>
 * Note that this class must be registered in a resource file called
 * {@code META-INF/services/com.exasol.adapter.AdapterFactory} in order for the {@link ServiceLoader} to find it.
 */
public class DynamodbAdapterFactory implements AdapterFactory {

    @Override
    public Set<String> getSupportedAdapterNames() {
        return Set.of(ADAPTER_NAME);
    }

    @Override
    public VirtualSchemaAdapter createAdapter() {
        return new DynamodbAdapter();
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
