# Virtual Schema for Amazon DynamoDB 4.0.3, released 2026-10-26

Code name: Dependency security fixes

## Summary

This release fixes 29 dependency vulnerabilities, including memory exhaustion, request smuggling, and certificate validation issues.

## Security

* #259: Fixed CVE-2026-110257 (uncontrolled memory allocation) in dependency `org.ow2.asm:asm:jar:9.10.1:test`
* #260: Fixed CVE-2026-110258 (excessive memory allocation) in dependency `org.ow2.asm:asm:jar:9.10.1:test`
* #261: Fixed CVE-2026-110259 (uncontrolled recursion) in dependency `org.ow2.asm:asm:jar:9.10.1:test`
* #262: Fixed CVE-2026-110260 (quadratic memory allocation) in dependency `org.ow2.asm:asm:jar:9.10.1:test`
* #263: Fixed CVE-2026-110261 (algorithmic complexity) in dependency `org.ow2.asm:asm:jar:9.10.1:test`
* #264: Fixed CVE-2026-110263 (uncontrolled recursion) in dependency `org.ow2.asm:asm:jar:9.10.1:test`
* #257: Fixed CVE-2026-110262 (excessive memory allocation) in dependency `org.ow2.asm:asm-commons:jar:9.10.1:test`
* #258: Fixed CVE-2026-110270 (exponential memory allocation) in dependency `org.ow2.asm:asm-commons:jar:9.10.1:test`
* #232: Fixed CVE-2026-75595 (mutual TLS authentication bypass) in dependency `io.netty:netty-handler:jar:4.1.136.Final:runtime`
* #230: Fixed CVE-2026-75596 (inefficient algorithmic complexity) in dependency `io.netty:netty-handler:jar:4.1.136.Final:runtime`
* #233: Fixed CVE-2026-62243 (certificate hostname mismatch validation) in dependency `io.netty:netty-handler:jar:4.1.136.Final:runtime`
* #238: Fixed CVE-2026-89044 (HTTP request smuggling) in dependency `io.netty:netty-codec-http2:jar:4.1.136.Final:runtime`
* #252: Fixed CVE-2026-93492 (HTTP/2 header table resource exhaustion) in dependency `io.netty:netty-codec-http2:jar:4.1.136.Final:runtime`
* #253: Fixed CVE-2026-93567 (incorrect CONNECT authority handling) in dependency `io.netty:netty-codec-http2:jar:4.1.136.Final:runtime`
* #254: Fixed CVE-2026-93568 (HTTP/2 CONNECT protocol misinterpretation) in dependency `io.netty:netty-codec-http2:jar:4.1.136.Final:runtime`
* #255: Fixed CVE-2026-93579 (HTTP/2 header validation bypass) in dependency `io.netty:netty-codec-http2:jar:4.1.136.Final:runtime`
* #226: Fixed CVE-2026-59903 (sensitive information in cache) in dependency `io.netty:netty-codec-http:jar:4.1.136.Final:runtime`
* #237: Fixed CVE-2026-89044 (HTTP request smuggling) in dependency `io.netty:netty-codec-http:jar:4.1.136.Final:runtime`
* #244: Fixed CVE-2026-93488 (unbounded concurrent SPDY streams) in dependency `io.netty:netty-codec-http:jar:4.1.136.Final:runtime`
* #245: Fixed CVE-2026-93491 (unbounded HTTP pipeline queue) in dependency `io.netty:netty-codec-http:jar:4.1.136.Final:runtime`
* #246: Fixed CVE-2026-93558 (unbounded WebSocket extension queue) in dependency `io.netty:netty-codec-http:jar:4.1.136.Final:runtime`
* #247: Fixed CVE-2026-93562 (HTTP request smuggling) in dependency `io.netty:netty-codec-http:jar:4.1.136.Final:runtime`
* #248: Fixed CVE-2026-93565 (RTSP method token smuggling) in dependency `io.netty:netty-codec-http:jar:4.1.136.Final:runtime`
* #249: Fixed CVE-2026-93574 (HTTP request smuggling) in dependency `io.netty:netty-codec-http:jar:4.1.136.Final:runtime`
* #250: Fixed CVE-2026-93566 (HTTP request/response smuggling) in dependency `io.netty:netty-codec-http:jar:4.1.136.Final:runtime`
* #251: Fixed CVE-2026-93573 (HTTP interpretation conflict) in dependency `io.netty:netty-codec-http:jar:4.1.136.Final:runtime`
* #243: Fixed CVE-2026-100666 (HTTP response desynchronization) in dependency `io.netty:netty-codec-http:jar:4.1.136.Final:runtime`
* #236: Fixed CVE-2026-86231 (certificate revocation validation) in dependency `com.github.mwiede:jsch:jar:2.28.5:test`
* #228: Fixed CVE-2026-64607 (connection not released after invalid encoding) in dependency `org.apache.httpcomponents.client5:httpclient5:jar:5.6.2:runtime`
* #234: Fixed CVE-2026-71290 (TLS hostname verification bypass) in dependency `org.apache.httpcomponents.client5:httpclient5:jar:5.6.2:runtime`

## Dependency Updates

### Compile Dependency Updates

* Updated `com.exasol:virtual-schema-common-document:12.0.3` to `12.1.2`
* Updated `software.amazon.awssdk:dynamodb:2.49.4` to `2.57.3`

### Runtime Dependency Updates

* Updated `org.slf4j:slf4j-jdk14:2.0.18` to `2.0.20`

### Test Dependency Updates

* Updated `com.exasol:exasol-testcontainers:8.0.1` to `8.0.2`
* Updated `com.exasol:hamcrest-resultset-matcher:1.7.3` to `1.7.4`
* Updated `com.exasol:test-db-builder-java:4.0.1` to `4.0.3`
* Updated `com.exasol:udf-debugging-java:0.6.20` to `0.6.22`
* Updated `com.exasol:virtual-schema-common-document:12.0.3` to `12.1.2`
* Updated `org.mockito:mockito-junit-jupiter:5.23.0` to `5.24.0`

### Plugin Dependency Updates

* Updated `com.exasol:error-code-crawler-maven-plugin:2.1.0` to `2.1.2`
* Updated `com.exasol:project-keeper-maven-plugin:5.7.4` to `5.8.1`
* Updated `io.github.git-commit-id:git-commit-id-maven-plugin:10.0.0` to `10.0.1`
* Updated `org.apache.maven.plugins:maven-artifact-plugin:3.6.1` to `3.7.0`
* Updated `org.apache.maven.plugins:maven-compiler-plugin:3.15.0` to `3.16.0`
* Updated `org.apache.maven.plugins:maven-failsafe-plugin:3.5.6` to `3.6.0`
* Updated `org.apache.maven.plugins:maven-install-plugin:3.1.4` to `3.2.0`
* Updated `org.apache.maven.plugins:maven-jar-plugin:3.5.0` to `3.5.1`
* Updated `org.apache.maven.plugins:maven-surefire-plugin:3.5.6` to `3.6.0`
* Updated `org.apache.maven.plugins:maven-toolchains-plugin:3.2.0` to `3.3.0`
* Updated `org.codehaus.mojo:flatten-maven-plugin:1.7.3` to `1.8.0`
* Updated `org.codehaus.mojo:versions-maven-plugin:2.21.0` to `2.23.0`
* Updated `org.sonarsource.scanner.maven:sonar-maven-plugin:5.7.0.6970` to `5.8.0.7211`
