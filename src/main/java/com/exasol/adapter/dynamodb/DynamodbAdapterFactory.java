package com.exasol.adapter.dynamodb;

import com.exasol.adapter.AdapterFactory;
import com.exasol.adapter.VirtualSchemaAdapter;
import com.exasol.logging.VersionCollector;

import java.util.HashSet;
import java.util.ServiceLoader;
import java.util.Set;

/**
 * This class implements a factory for the {@link DynamodbAdapter}.
 * <p>
 * Note that this class must be registered in a resource file called
 * <code>META-INF/services/com.exasol.adapter.AdapterFactory</code> in order for
 * the {@link ServiceLoader} to find it.
 */
public class DynamodbAdapterFactory implements AdapterFactory {
	private static final String ADAPTER_NAME = "Dynamodb Adapter";

	@Override
	public Set<String> getSupportedAdapterNames() {
		final Set<String> supportedNames = new HashSet<>();
		supportedNames.add("Dynamodb");
		return supportedNames;
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
