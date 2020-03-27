package com.exasol.dynamodb;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;

/**
 * Helper functions for creating a DynamoDB connection.
 */
public class DynamodbConnectionUtil {
	private DynamodbConnectionUtil() {

	}

	/**
	 * Creates a AmazonDynamoDB (low level api client) for a given uri, user and
	 * key.
	 *
	 * @param uri
	 *            either aws:<REGION> or address of local DynamoDB server (e.g.
	 *            http://localhost:8000)
	 * @param user
	 *            aws credential id
	 * @param key
	 *            aws credential key
	 * @return AmazonDynamoDB (low level api client)
	 */
	public static AmazonDynamoDB getLowLevelConnection(final String uri, final String user, final String key) {

		final String AWS_PREFIX = "aws:";
		final String AWS_LOCAL_REGION = "eu-central-1";
		final BasicAWSCredentials awsCredentials = new BasicAWSCredentials(user, key);
		final AmazonDynamoDBClientBuilder clientBuilder = AmazonDynamoDBClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(awsCredentials));
		if (uri.startsWith(AWS_PREFIX)) {
			clientBuilder.withRegion(uri.replace(AWS_PREFIX, ""));
		} else {
			clientBuilder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(uri, AWS_LOCAL_REGION));
		}
		return clientBuilder.build();
	}

	/**
	 * Creates a DynamoDB (document api client) for a given uri, user and key. for
	 * details see {@link #getLowLevelConnection(String, String, String)}.
	 *
	 * @return DynamoDB (document api client)
	 */
	public static DynamoDB getDocumentConnection(final String uri, final String user, final String key) {
		return new DynamoDB(getLowLevelConnection(uri, user, key));
	}
}
