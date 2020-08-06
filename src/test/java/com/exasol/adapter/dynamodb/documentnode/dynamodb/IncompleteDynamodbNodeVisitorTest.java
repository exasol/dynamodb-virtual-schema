package com.exasol.adapter.dynamodb.documentnode.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Collections;

import org.junit.jupiter.api.Test;

import software.amazon.awssdk.core.SdkBytes;

class IncompleteDynamodbNodeVisitorTest {

    @Test
    void testVisitString() {
        final Visitor visitor = new Visitor();
        new DynamodbString("").accept(visitor);
        assertThat(visitor.getVisited(), equalTo("String"));
    }

    @Test
    void testVisitNumber() {
        final Visitor visitor = new Visitor();
        new DynamodbNumber("1").accept(visitor);
        assertThat(visitor.getVisited(), equalTo("Number"));
    }

    @Test
    void testVisitBinary() {
        final Visitor visitor = new Visitor();
        new DynamodbBinary(SdkBytes.fromUtf8String("")).accept(visitor);
        assertThat(visitor.getVisited(), equalTo("Binary"));
    }

    @Test
    void testVisitBool() {
        final Visitor visitor = new Visitor();
        new DynamodbBoolean(true).accept(visitor);
        assertThat(visitor.getVisited(), equalTo("Boolean"));
    }

    @Test
    void testVisitStringSet() {
        final Visitor visitor = new Visitor();
        new DynamodbStringSet(Collections.emptyList()).accept(visitor);
        assertThat(visitor.getVisited(), equalTo("StringSet"));
    }

    @Test
    void testVisitNumberSet() {
        final Visitor visitor = new Visitor();
        new DynamodbNumberSet(Collections.emptyList()).accept(visitor);
        assertThat(visitor.getVisited(), equalTo("NumberSet"));
    }

    @Test
    void testVisitBinarySet() {
        final Visitor visitor = new Visitor();
        new DynamodbBinarySet(Collections.emptyList()).accept(visitor);
        assertThat(visitor.getVisited(), equalTo("BinarySet"));
    }

    @Test
    void testVisitList() {
        final Visitor visitor = new Visitor();
        new DynamodbList(Collections.emptyList()).accept(visitor);
        assertThat(visitor.getVisited(), equalTo("List"));
    }

    @Test
    void testVisitMap() {
        final Visitor visitor = new Visitor();
        new DynamodbMap(Collections.emptyMap()).accept(visitor);
        assertThat(visitor.getVisited(), equalTo("Map"));
    }

    @Test
    void testVisitNull() {
        final Visitor visitor = new Visitor();
        new DynamodbNull().accept(visitor);
        assertThat(visitor.getVisited(), equalTo("Null"));
    }

    private static class Visitor implements IncompleteDynamodbNodeVisitor {
        String visited = "";

        @Override
        public void defaultVisit(final String typeName) {
            this.visited = typeName;
        }

        private String getVisited() {
            return this.visited;
        }
    }
}