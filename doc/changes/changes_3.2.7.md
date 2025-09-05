# Virtual Schema for Amazon DynamoDB 3.2.7, released 2025-??-??

Code name: Fixed vulnerability CVE-2025-58057 in io.netty:netty-codec:jar:4.1.118.Final:runtime

## Summary

This release fixes the following vulnerability:

### CVE-2025-58057 (CWE-409) in dependency `io.netty:netty-codec:jar:4.1.118.Final:runtime`
netty-codec - Improper Handling of Highly Compressed Data (Data Amplification)
#### References
* https://ossindex.sonatype.org/vulnerability/CVE-2025-58057?component-type=maven&component-name=io.netty%2Fnetty-codec&utm_source=ossindex-client&utm_medium=integration&utm_content=1.8.1
* http://web.nvd.nist.gov/view/vuln/detail?vulnId=CVE-2025-58057
* https://github.com/netty/netty/security/advisories/GHSA-3p8m-j85q-pgmj

## Security

* #202: Fixed vulnerability CVE-2025-58057 in dependency `io.netty:netty-codec:jar:4.1.118.Final:runtime`

## Dependency Updates

### Compile Dependency Updates

* Updated `com.exasol:virtual-schema-common-document:11.0.3` to `11.0.6`
* Updated `software.amazon.awssdk:dynamodb:2.31.55` to `2.33.3`

### Test Dependency Updates

* Updated `com.exasol:hamcrest-resultset-matcher:1.7.1` to `1.7.2`
* Updated `com.exasol:test-db-builder-java:3.6.1` to `3.6.3`
* Updated `com.exasol:udf-debugging-java:0.6.16` to `0.6.17`
* Updated `com.exasol:virtual-schema-common-document:11.0.3` to `11.0.6`
* Updated `nl.jqno.equalsverifier:equalsverifier:3.19` to `4.1`
* Updated `org.junit.jupiter:junit-jupiter-params:5.13.0` to `5.13.4`
* Updated `org.mockito:mockito-junit-jupiter:5.18.0` to `5.19.0`
* Updated `org.testcontainers:junit-jupiter:1.21.1` to `1.21.3`
