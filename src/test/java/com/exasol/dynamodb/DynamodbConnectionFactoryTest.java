package com.exasol.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.lang.reflect.Field;
import java.net.URI;

import org.junit.jupiter.api.Test;

import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;

public class DynamodbConnectionFactoryTest {

    Object getPrivateField(final String name, final AmazonDynamoDB connection)
            throws NoSuchFieldException, IllegalAccessException {
        final AmazonWebServiceClient client = (AmazonWebServiceClient) connection;
        final Field privateStringField = AmazonWebServiceClient.class.getDeclaredField(name);
        privateStringField.setAccessible(true);
        return privateStringField.get(client);
    }

    @Test
    void testGetLowLevelConnectionLocal() throws NoSuchFieldException, IllegalAccessException {
        final String uri = "http://127.0.0.1:1234";
        final AmazonDynamoDB connection = new DynamodbConnectionFactory().getLowLevelConnection(uri, "", "");
        final URI endpoint = (URI) getPrivateField("endpoint", connection);
        final String region = (String) getPrivateField("signerRegionOverride", connection);
        assertThat(endpoint.toString(), equalTo(uri));
        assertThat(region, equalTo("eu-central-1"));
    }

    @Test
    void testGetLowLevelConnectionAws() throws NoSuchFieldException, IllegalAccessException {
        final String uri = "aws:ca-central-1";
        final AmazonDynamoDB connection = new DynamodbConnectionFactory().getLowLevelConnection(uri, "", "");
        final URI endpoint = (URI) getPrivateField("endpoint", connection);
        assertThat(endpoint.toString(), equalTo("https://dynamodb.ca-central-1.amazonaws.com"));
    }
}
