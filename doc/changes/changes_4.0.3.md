# Virtual Schema for Amazon DynamoDB 4.0.3, released 2026-??-??

Code name: Fixed vulnerability CVE-2026-75596 in io.netty:netty-handler:jar:4.1.136.Final:runtime

## Summary

This release fixes the following vulnerability:

### CVE-2026-75596 (CWE-407) in dependency `io.netty:netty-handler:jar:4.1.136.Final:runtime`
io.netty:netty-handler - Inefficient Algorithmic Complexity
#### References
* https://guide.sonatype.com/vulnerability/CVE-2026-75596?component-type=maven&component-name=io.netty%2Fnetty-handler&utm_source=ossindex-client&utm_medium=integration&utm_content=1.8.1
* http://web.nvd.nist.gov/view/vuln/detail?vulnId=CVE-2026-75596
* https://github.com/netty/netty/security/advisories/GHSA-fccg-mwvh-qqg4

## Security

* #230: Fixed vulnerability CVE-2026-75596 in dependency `io.netty:netty-handler:jar:4.1.136.Final:runtime`

## Dependency Updates

### Compile Dependency Updates

* Updated `com.exasol:virtual-schema-common-document:12.0.3` to `12.1.0`
* Updated `software.amazon.awssdk:dynamodb:2.49.4` to `2.54.2`

### Test Dependency Updates

* Updated `com.exasol:test-db-builder-java:4.0.1` to `4.0.2`
* Updated `com.exasol:virtual-schema-common-document:12.0.3` to `12.1.0`
* Updated `nl.jqno.equalsverifier:equalsverifier:3.19.4` to `4.5.1`
* Updated `org.junit.jupiter:junit-jupiter-params:5.14.4` to `6.1.3`
