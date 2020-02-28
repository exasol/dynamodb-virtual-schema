package util;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exasol.adapter.dynamodb.DynamodbAdapterTestLocalIT;
import com.exasol.bucketfs.Bucket;
import com.exasol.bucketfs.BucketAccessException;
import com.exasol.containers.ExasolContainer;
import com.github.dockerjava.api.model.ContainerNetwork;

/**
 * Test utils for exasol database
 */
public class ExasolTestUtils {

	private static final Logger LOGGER = LoggerFactory.getLogger(DynamodbAdapterTestLocalIT.class);

	private static final String VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION = "dynamodb-virtual-schemas-adapter-dist-0.1.0.jar";
	private static final Path PATH_TO_VIRTUAL_SCHEMAS_JAR = Path.of("target", VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION);
	public static final String ADAPTER_SCHEMA = "ADAPTER";
	public static final String DYNAMODB_ADAPTER = "DYNAMODB_ADAPTER";

	private final ExasolContainer<? extends ExasolContainer<?>> container;
	private final Statement statement;

	/**
	 * Constructor
	 * 
	 * @param container
	 *            exasol test container
	 * @throws SQLException
	 */
	public ExasolTestUtils(final ExasolContainer<? extends ExasolContainer<?>> container) throws SQLException {
		final Connection connection = container.createConnectionForUser(container.getUsername(),
				container.getPassword());
		this.statement = connection.createStatement();
		this.container = container;
	}

	/**
	 * @return SQL Statement for running jdbc queries on exasol test container given
	 *         in constructor
	 */
	public Statement getStatement() {
		return this.statement;
	}

	/**
	 * Uploads the dynamodb adapter jar to the exasol test container
	 * 
	 * @throws InterruptedException
	 * @throws BucketAccessException
	 * @throws TimeoutException
	 */
	public void uploadDynamodbAdapterJar() throws InterruptedException, BucketAccessException, TimeoutException {
		final Bucket bucket = this.container.getDefaultBucket();
		bucket.uploadFile(PATH_TO_VIRTUAL_SCHEMAS_JAR, VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION);
	}

	/**
	 * creates a schema on the exasol test container
	 * 
	 * @param schemaName
	 * @throws SQLException
	 */
	public void createTestSchema(final String schemaName) throws SQLException {
		this.statement.execute("CREATE SCHEMA " + schemaName);
	}

	/**
	 * Runs CREATE CONNECTION on the exasol test container
	 * 
	 * @param name
	 * @param to
	 * @param user
	 * @param pass
	 * @throws SQLException
	 */
	public void createConnection(final String name, final String to, final String user, final String pass)
			throws SQLException {
		this.statement.execute(
				"CREATE CONNECTION " + name + " TO '" + to + "' USER '" + user + "' IDENTIFIED BY '" + pass + "';");
	}

	/**
	 * Runs CREATE OR REPLACE JAVA ADAPTER SCRIPT on the exasol test container with
	 * the DynamoDB Virtual Schema adapter jar
	 * 
	 * @throws SQLException
	 */
	public void createAdapterScript() throws SQLException {
		this.createTestSchema(ADAPTER_SCHEMA);
		// noinspection SpellCheckingInspection
		this.statement.execute("CREATE OR REPLACE JAVA ADAPTER SCRIPT " + ADAPTER_SCHEMA + "." + DYNAMODB_ADAPTER
				+ " AS\n" + "    %scriptclass com.exasol.adapter.RequestDispatcher;\n"
				+ "    %jar /buckets/bfsdefault/default/" + VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION + ";\n" + "/");
	}

	/**
	 * hacky method for retrieving the host address for access from inside the
	 * docker container
	 */
	private String getTestHostIpAddress() {
		final Map<String, ContainerNetwork> networks = this.container.getContainerInfo().getNetworkSettings()
				.getNetworks();
		if (networks.size() == 0)
			return null;
		return networks.values().iterator().next().getGateway();
	}

	/**
	 * Runs CREATE VIRTUAL SCHEMA on exasol test container
	 * 
	 * @param name
	 *            name for the newly created schema
	 * @param dynamodbConnection
	 *            name of the connection to use
	 * @throws SQLException
	 */
	public void createDynamodbVirtualSchema(final String name, final String dynamodbConnection) throws SQLException {
		LOGGER.info(getTestHostIpAddress());
		String createStatement = "CREATE VIRTUAL SCHEMA " + name + "\n" + "    USING " + ADAPTER_SCHEMA + "."
				+ DYNAMODB_ADAPTER + " WITH\n" + "    CONNECTION_NAME = '" + dynamodbConnection + "'\n"
				+ "   SQL_DIALECT     = 'DynamoDB'";
		final String hostIp = getTestHostIpAddress();
		if (hostIp != null) {
			createStatement += "\n   DEBUG_ADDRESS   = '" + getTestHostIpAddress() + ":3000'\n"
					+ "   LOG_LEVEL       =  'ALL'";
		}
		createStatement += ";";
		this.statement.execute(createStatement);
	}
}
