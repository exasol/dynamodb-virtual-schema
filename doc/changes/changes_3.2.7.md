# Virtual Schema for Amazon DynamoDB 3.2.7, released 2025-09-22

Code name: Fixed vulnerabilities in Netty

## Summary

This release fixes the following vulnerabilities:

### CVE-2025-58056 (CWE-444) in dependency `io.netty:netty-codec-http:jar:4.1.118.Final:runtime`
Netty is an asynchronous event-driven network application framework for development of maintainable high performance protocol servers and clients. In versions 4.1.124.Final, and 4.2.0.Alpha3 through 4.2.4.Final, Netty incorrectly accepts standalone newline characters (LF) as a chunk-size line terminator, regardless of a preceding carriage return (CR), instead of requiring CRLF per HTTP/1.1 standards. When combined with reverse proxies that parse LF differently (treating it as part of the chunk extension), attackers can craft requests that the proxy sees as one request but Netty processes as two, enabling request smuggling attacks. This is fixed in versions 4.1.125.Final and 4.2.5.Final.
#### References
* https://ossindex.sonatype.org/vulnerability/CVE-2025-58056?component-type=maven&component-name=io.netty%2Fnetty-codec-http&utm_source=ossindex-client&utm_medium=integration&utm_content=1.8.1
* http://web.nvd.nist.gov/view/vuln/detail?vulnId=CVE-2025-58056
* https://github.com/netty/netty/security/advisories/GHSA-fghv-69vj-qj49

### CVE-2025-58057 (CWE-409) in dependency `io.netty:netty-codec:jar:4.1.118.Final:runtime`
netty-codec - Improper Handling of Highly Compressed Data (Data Amplification)
#### References
* https://ossindex.sonatype.org/vulnerability/CVE-2025-58057?component-type=maven&component-name=io.netty%2Fnetty-codec&utm_source=ossindex-client&utm_medium=integration&utm_content=1.8.1
* http://web.nvd.nist.gov/view/vuln/detail?vulnId=CVE-2025-58057
* https://github.com/netty/netty/security/advisories/GHSA-3p8m-j85q-pgmj

## Security

* #204: Fixed vulnerability CVE-2025-58056 in dependency `io.netty:netty-codec-http:jar:4.1.118.Final:runtime`
* #202: Fixed vulnerability CVE-2025-58057 in dependency `io.netty:netty-codec:jar:4.1.118.Final:runtime`

## Dependency Updates

### Compile Dependency Updates

* Updated `com.exasol:virtual-schema-common-document:11.0.3` to `11.0.6`
* Updated `software.amazon.awssdk:dynamodb:2.31.55` to `2.34.0`

### Test Dependency Updates

* Updated `com.exasol:hamcrest-resultset-matcher:1.7.1` to `1.7.2`
* Updated `com.exasol:test-db-builder-java:3.6.1` to `3.6.3`
* Updated `com.exasol:udf-debugging-java:0.6.16` to `0.6.17`
* Updated `com.exasol:virtual-schema-common-document:11.0.3` to `11.0.6`
* Updated `nl.jqno.equalsverifier:equalsverifier:3.19` to `3.19.4`
* Updated `org.junit.jupiter:junit-jupiter-params:5.13.0` to `5.13.4`
* Updated `org.mockito:mockito-junit-jupiter:5.18.0` to `5.20.0`
* Updated `org.testcontainers:junit-jupiter:1.21.1` to `1.21.3`
