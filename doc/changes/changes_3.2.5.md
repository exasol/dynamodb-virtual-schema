# Virtual Schema for Amazon DynamoDB 3.2.5, released 2025-??-??

Code name: Fixed vulnerability CVE-2024-55551 in com.exasol:exasol-jdbc:jar:24.2.1:test

## Summary

This release fixes the following vulnerability:

### CVE-2024-55551 (CWE-94) in dependency `com.exasol:exasol-jdbc:jar:24.2.1:test`
An issue was discovered in Exasol jdbc driver 24.2.0. Attackers can inject malicious parameters into the JDBC URL, triggering JNDI injection during the process when the JDBC Driver uses this URL to connect to the database. This can further lead to remote code execution vulnerability.
#### References
* https://ossindex.sonatype.org/vulnerability/CVE-2024-55551?component-type=maven&component-name=com.exasol%2Fexasol-jdbc&utm_source=ossindex-client&utm_medium=integration&utm_content=1.8.1
* http://web.nvd.nist.gov/view/vuln/detail?vulnId=CVE-2024-55551
* https://gist.github.com/azraelxuemo/9565ec9219e0c3e9afd5474904c39d0f

## Security

* #196: Fixed vulnerability CVE-2024-55551 in dependency `com.exasol:exasol-jdbc:jar:24.2.1:test`

## Dependency Updates

### Compile Dependency Updates

* Updated `software.amazon.awssdk:dynamodb:2.30.18` to `2.31.5`

### Runtime Dependency Updates

* Updated `org.slf4j:slf4j-jdk14:2.0.16` to `2.0.17`

### Test Dependency Updates

* Updated `com.exasol:exasol-testcontainers:7.1.3` to `7.1.4`
* Updated `com.exasol:udf-debugging-java:0.6.14` to `0.6.15`
* Updated `nl.jqno.equalsverifier:equalsverifier:3.19` to `3.19.2`
* Updated `org.junit.jupiter:junit-jupiter-params:5.11.4` to `5.12.1`
* Updated `org.mockito:mockito-junit-jupiter:5.15.2` to `5.16.1`
* Updated `org.testcontainers:junit-jupiter:1.20.4` to `1.20.6`

### Plugin Dependency Updates

* Updated `com.exasol:project-keeper-maven-plugin:4.5.0` to `5.0.0`
