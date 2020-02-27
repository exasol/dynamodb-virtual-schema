package util;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import com.exasol.adapter.dynamodb.DynamodbAdapterTestLocalIT;
import com.github.dockerjava.api.model.ContainerNetwork;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.*;

/*
    Test utils for testing dynamodb
 */
public class DynamodbTestUtils {
	// default credentails for dynamodb docker
	private static final String LOCAL_DYNAMO_USER = "fakeMyKeyId";
	private static final String LOCAL_DYNAMO_PASS = "fakeSecretAccessKey";

	private final DynamoDbClient dynamoClient;
	private final String dynamoUrl;
	private final String localUrl;

	private final String dynamoUser;
	private final String dynamoPass;

	private static final Logger LOGGER = LoggerFactory.getLogger(DynamodbAdapterTestLocalIT.class);

	/**
	 * Creates a DynamodbTestUtils for aws with credentials from env var
	 */
	public DynamodbTestUtils() {
		this(DefaultCredentialsProvider.create().resolveCredentials());
	}

	/*
	 * Creates a DynamodbTestUtils for aws with credentials from env var
	 */
	public DynamodbTestUtils(final AwsCredentials awsCredentials) {
		this(awsCredentials.accessKeyId(), awsCredentials.secretAccessKey());
	}

	/*
	 * Creates an DynamodbTestUtils instance with default login credentials for the
	 * local dynamodb docker instance
	 */
	public DynamodbTestUtils(final GenericContainer localDynamo, final Network dockerNetwork) throws Exception {
		this(getLocalUrlForLocalDynamodb(localDynamo), getDockerNetworkURlForLocalDynamodb(localDynamo, dockerNetwork),
				LOCAL_DYNAMO_USER, LOCAL_DYNAMO_PASS);
	}

	private static String getDockerNetworkURlForLocalDynamodb(final GenericContainer localDynamo,
			final Network thisNetwork) throws Exception {
		final String address = localDynamo.getContainerIpAddress();
		final Map<String, ContainerNetwork> networks = localDynamo.getContainerInfo().getNetworkSettings()
				.getNetworks();
		for (final ContainerNetwork network : networks.values()) {
			if (thisNetwork.getId().equals(network.getNetworkID())) {
				return "http://" + network.getIpAddress() + ":8000";
			}
		}

		throw new Exception("no network found");
	}

	private static String getLocalUrlForLocalDynamodb(final GenericContainer localDynamo) {
		return "http://127.0.0.1:" + localDynamo.getFirstMappedPort();
	}

	/*
	 * Create DynamodbTestUtils for aws connection
	 */
	public DynamodbTestUtils(final String user, final String pass) {
		this("aws", "aws", user, pass);
	}

	private DynamodbTestUtils(final String localUrl, final String dynamoUrl, final String user, final String pass) {
		this.localUrl = localUrl;
		this.dynamoUrl = dynamoUrl;
		this.dynamoUser = user;
		this.dynamoPass = pass;
		final StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider
				.create(AwsBasicCredentials.create(user, pass));
		final DynamoDbClientBuilder cilentBuilder = DynamoDbClient.builder().region(Region.EU_CENTRAL_1)
				.credentialsProvider(credentialsProvider);
		if (!this.dynamoUrl.equals("aws")) {
			cilentBuilder.endpointOverride(URI.create(this.dynamoUrl));
		}
		this.dynamoClient = cilentBuilder.build();
	}

	public void pushItem() {
		final HashMap<String, AttributeValue> itemValues = new HashMap<String, AttributeValue>();
		itemValues.put("isbn", AttributeValue.builder().s("12398439493").build());
		itemValues.put("name", AttributeValue.builder().s("dummes Buch").build());
		final PutItemRequest request = PutItemRequest.builder().tableName("JB_Books").item(itemValues).build();
		this.dynamoClient.putItem(request);
	}

	public int scan(final String tableName) {
		final ScanResponse res = this.dynamoClient.scan(ScanRequest.builder().tableName(tableName).build());
		LOGGER.trace(res.toString());
		return res.count();
	}

	public void createTable(final String tableName, final String keyName) {
		final CreateTableRequest request = CreateTableRequest.builder().tableName(tableName)
				.attributeDefinitions(AttributeDefinition.builder().attributeName(keyName)
						.attributeType(ScalarAttributeType.S).build())
				.keySchema(KeySchemaElement.builder().keyType(KeyType.HASH).attributeName(keyName).build())
				.provisionedThroughput(
						ProvisionedThroughput.builder().readCapacityUnits(1L).writeCapacityUnits(1L).build())
				.build();
		this.dynamoClient.createTable(request);
	}

	public void importData(final File asset) throws IOException, InterruptedException {
		final Runtime runtime = Runtime.getRuntime();
		String importCommand = "aws dynamodb batch-write-item --request-items file://" + asset.getPath();
		if (!this.localUrl.equals("aws")) {
			importCommand += " --endpoint-url " + this.localUrl;
		}

		System.out.println(importCommand);
		final Process process = runtime.exec(importCommand);
		final InputStream stderr = process.getErrorStream();
		final InputStreamReader inputStreamReader = new InputStreamReader(stderr);
		final BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

		String line = null;
		while ((line = bufferedReader.readLine()) != null) {
			LOGGER.error(line);
		}
		process.waitFor();
	}

	public String getDynamoUrl() {
		return this.dynamoUrl;
	}

	public String getDynamoUser() {
		return this.dynamoUser;
	}

	public String getDynamoPass() {
		return this.dynamoPass;
	}
}
