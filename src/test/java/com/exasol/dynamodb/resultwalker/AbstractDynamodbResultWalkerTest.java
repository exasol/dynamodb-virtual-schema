package com.exasol.dynamodb.resultwalker;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

public class AbstractDynamodbResultWalkerTest {
    Map<String, AttributeValue> getTestData() {
        final AttributeValue isbn = new AttributeValue();
        isbn.setS("1234");
        final AttributeValue name1 = new AttributeValue();
        name1.setS("Tom");
        final AttributeValue name2 = new AttributeValue();
        name1.setS("Simon");
        final AttributeValue authors = new AttributeValue();
        authors.setL(List.of(name1, name2));
        final AttributeValue publisherName = new AttributeValue();
        publisherName.setS("Exasol");
        final AttributeValue publisher = new AttributeValue();
        publisher.setM(Map.of("name", publisherName));
        return Map.of("isbn", isbn, "authors", authors, "publisher", publisher);
    }

    @Test
    void testIdentityWalker() throws DynamodbResultWalkerException {
        final Map<String, AttributeValue> testData = getTestData();
        final IdentityDynamodbResultWalker walker = new IdentityDynamodbResultWalker();
        assertThat(walker.walk(testData).getM(), equalTo(testData));
    }

    @Test
    void testChainedIdentityWalker() throws DynamodbResultWalkerException {
        final Map<String, AttributeValue> testData = getTestData();
        final IdentityDynamodbResultWalker walker = new IdentityDynamodbResultWalker(
                new IdentityDynamodbResultWalker());
        assertThat(walker.walk(testData).getM(), equalTo(testData));
    }

    @Test
    void testObjectWalker() throws DynamodbResultWalkerException {
        final Map<String, AttributeValue> testData = getTestData();
        final ObjectDynamodbResultWalker walker = new ObjectDynamodbResultWalker("isbn", null);
        assertThat(walker.walk(testData), equalTo(testData.get("isbn")));
    }

    @Test
    void testChainedObjectWalker() throws DynamodbResultWalkerException {
        final Map<String, AttributeValue> testData = getTestData();
        final ObjectDynamodbResultWalker walker = new ObjectDynamodbResultWalker("publisher",
                new ObjectDynamodbResultWalker("name", null));
        assertThat(walker.walk(testData), equalTo(testData.get("publisher").getM().get("name")));
    }

    @Test
    void testObjectWalkerException() {
        final ObjectDynamodbResultWalker walker = new ObjectDynamodbResultWalker("isbn", null);
        final DynamodbResultWalkerException exception = assertThrows(DynamodbResultWalkerException.class,
                () -> walker.walk(Collections.emptyMap()));
        assertThat(exception.getCurrentPath(), equalTo(""));
    }

    @Test
    void testObjectWalkerLookupExceptionInChainedObjectWalker() {
        final ObjectDynamodbResultWalker walker = new ObjectDynamodbResultWalker("publisher",
                new ObjectDynamodbResultWalker("unknownProperty", null));
        final ObjectDynamodbResultWalker.LookupException exception = assertThrows(
                ObjectDynamodbResultWalker.LookupException.class, () -> walker.walk(getTestData()));
        assertThat(exception.getCurrentPath(), equalTo("/publisher"));
        assertThat(exception.getMissingLookupKey(), equalTo("unknownProperty"));
    }

    @Test
    void testObjectWalkerExceptionInChainedObjectWalker() {
        final ObjectDynamodbResultWalker walker = new ObjectDynamodbResultWalker("publisher",
                new ObjectDynamodbResultWalker("name", new ObjectDynamodbResultWalker("unknownProperty", null)));
        final DynamodbResultWalkerException exception = assertThrows(DynamodbResultWalkerException.class,
                () -> walker.walk(getTestData()));
        assertThat(exception.getCurrentPath(), equalTo("/publisher/name"));
    }
}
