# Virtual Schema for Amazon DynamoDB 4.0.3, released 2026-??-??

Code name: Fixed vulnerability CVE-2026-64607 in org.apache.httpcomponents.client5:httpclient5:jar:5.6.2:runtime

## Summary

This release fixes the following vulnerability:

### CVE-2026-64607 (CWE-772) in dependency `org.apache.httpcomponents.client5:httpclient5:jar:5.6.2:runtime`
HttpClient based on the classic i/o model fails to correctly release the underlying connection back to the connection manager if it encounters an invalid or unsupported `Content-Encoding` header value in the response message.Â Please note this defect does not affect HttpClient based on the async i/o model.

This issue affects Apache HttpComponents Client: from 5.0-alpha1 through 5.6.2.
#### References
* https://guide.sonatype.com/vulnerability/CVE-2026-64607?component-type=maven&component-name=org.apache.httpcomponents.client5%2Fhttpclient5&utm_source=ossindex-client&utm_medium=integration&utm_content=1.8.1
* http://web.nvd.nist.gov/view/vuln/detail?vulnId=CVE-2026-64607
* https://github.com/advisories/GHSA-hjcp-jmpx-g3qm

## Security

* #228: Fixed vulnerability CVE-2026-64607 in dependency `org.apache.httpcomponents.client5:httpclient5:jar:5.6.2:runtime`

## Dependency Updates

### Compile Dependency Updates

* Updated `com.exasol:virtual-schema-common-document:12.0.3` to `12.1.0`
* Updated `software.amazon.awssdk:dynamodb:2.49.4` to `2.53.1`

### Test Dependency Updates

* Updated `com.exasol:test-db-builder-java:4.0.1` to `4.0.2`
* Updated `com.exasol:virtual-schema-common-document:12.0.3` to `12.1.0`
* Updated `nl.jqno.equalsverifier:equalsverifier:3.19.4` to `4.5`
* Updated `org.junit.jupiter:junit-jupiter-params:5.14.4` to `6.1.3`
