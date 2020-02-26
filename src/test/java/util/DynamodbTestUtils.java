package util;

import com.exasol.adapter.dynamodb.DynamodbAdapter;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.testcontainers.containers.GenericContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;

/*
    Test utils for testing dynamodb
 */
public class DynamodbTestUtils {
    //default credentails for dynamodb docker
    public static final String LOCAL_DYNAMO_USER = "fakeMyKeyId";
    public static final String LOCAL_DYNAMO_PASS = "fakeSecretAccessKey";

    private DynamoDbClient client;
    private String url;


    /*
        Creates a DynamodbTestUtils for aws with credentails from env var
     */
    public DynamodbTestUtils(){
        this( System.getenv("AWS_ACCESS_KEY_ID"),System.getenv("AWS_SECRET_ACCESS_KEY"));
        /*
AwsSessionCredentials awsCreds = AwsSessionCredentials.create(
    "your_access_key_id_here",
    "your_secret_key_id_here",
    "your_session_token_here");
         */
    }

    /*
        Creates an DynamodbTestUtils instance with default login credentials for the local dynamodb docker instance
     */
    public DynamodbTestUtils(GenericContainer localDynamo){
        this( getUrlForLocalDynamodb(localDynamo), LOCAL_DYNAMO_USER,LOCAL_DYNAMO_PASS);
    }

    private static String getUrlForLocalDynamodb(GenericContainer localDynamo){
        String address = localDynamo.getContainerIpAddress();
        Integer port = localDynamo.getFirstMappedPort();
        address = "10.0.2.15"; //TODO beeter solution
        return "http://" + address + ":" + port;
    }

    /*
        Create DynamodbTestUtils for aws connection
     */
    public DynamodbTestUtils(String user, String pass){
        this("aws",user,pass);
    }


    private DynamodbTestUtils(String url, String user, String pass) {
        this.url = url;
        StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(user, pass));
        DynamoDbClientBuilder cilentBuilder = DynamoDbClient.builder().region(Region.EU_CENTRAL_1).credentialsProvider(credentialsProvider);
        if(!url.equals("aws")){
            cilentBuilder.endpointOverride(URI.create(url));
        }
        this.client = cilentBuilder.build();
    }



    public void pushItem(){
        HashMap<String, AttributeValue> itemValues = new HashMap<String, AttributeValue>();
        itemValues.put("isbn", AttributeValue.builder().s("12398439493").build());
        itemValues.put("name", AttributeValue.builder().s("dummes Buch").build());
        PutItemRequest request = PutItemRequest.builder().tableName("JB_Books").item(itemValues).build();
        client.putItem(request);
    }

    public int scan(String tableName){
        ScanResponse res = client.scan(ScanRequest.builder().tableName(tableName).build());
        System.out.println(res.toString());
        return res.count();
    }

    public void createTable(String tableName, String keyName){
        CreateTableRequest request = CreateTableRequest.builder().tableName(tableName)
                .attributeDefinitions(AttributeDefinition.builder()
                        .attributeName(keyName)
                        .attributeType(ScalarAttributeType.S)
                        .build())
                .keySchema(
                        KeySchemaElement.builder().keyType(KeyType.HASH).attributeName(keyName).build()
                ).provisionedThroughput(ProvisionedThroughput.builder()
                        .readCapacityUnits(new Long(1))
                        .writeCapacityUnits(new Long(1))
                        .build())
                .build();
        client.createTable(request);
    }

    public void importData(File asset) throws IOException, InterruptedException {
        Runtime rt = Runtime.getRuntime();
        String cmd = "aws dynamodb batch-write-item --request-items file://" + asset.getPath() + " --endpoint-url " + getUrl();
        System.out.println(cmd);
        Process proc = rt.exec(cmd);
        InputStream stderr = proc.getErrorStream();
        InputStreamReader isr = new InputStreamReader(stderr);
        BufferedReader br = new BufferedReader(isr);
        String line = null;
        while ( (line = br.readLine()) != null)
            System.out.println(line);

        proc.waitFor();
    }

    public String getUrl(){
        return this.url;
    }
}
