# Virtual Schema for Amazon DynamoDB 4.0.2, released 2026-??-??

Code name: Fixed vulnerabilities CVE-2026-59901, CVE-2026-59921, CVE-2026-56819

## Summary

This release fixes the following 3 vulnerabilities:

### CVE-2026-59901 (CWE-835) in dependency `io.netty:netty-codec:jar:4.1.135.Final:runtime`
netty-codec - Bzip2Decoder infinite loop DoS via malformed stream
#### References
* https://guide.sonatype.com/vulnerability/CVE-2026-59901?component-type=maven&component-name=io.netty%2Fnetty-codec&utm_source=ossindex-client&utm_medium=integration&utm_content=1.8.1
* http://web.nvd.nist.gov/view/vuln/detail?vulnId=CVE-2026-59901
* https://github.com/netty/netty/security/advisories/GHSA-558v-64gr-wgg4

### CVE-2026-59921 (CWE-93) in dependency `io.netty:netty-codec-http:jar:4.1.135.Final:runtime`
netty-codec-http - CRLF injection via multipart filename
#### References
* https://guide.sonatype.com/vulnerability/CVE-2026-59921?component-type=maven&component-name=io.netty%2Fnetty-codec-http&utm_source=ossindex-client&utm_medium=integration&utm_content=1.8.1
* http://web.nvd.nist.gov/view/vuln/detail?vulnId=CVE-2026-59921
* https://github.com/netty/netty/security/advisories/GHSA-gcjf-9mgh-3p7g

### CVE-2026-56819 (CWE-401) in dependency `io.netty:netty-codec-http2:jar:4.1.135.Final:runtime`
netty-codec-http2 - Memory leak in HTTP/2 decompressor
#### References
* https://guide.sonatype.com/vulnerability/CVE-2026-56819?component-type=maven&component-name=io.netty%2Fnetty-codec-http2&utm_source=ossindex-client&utm_medium=integration&utm_content=1.8.1
* http://web.nvd.nist.gov/view/vuln/detail?vulnId=CVE-2026-56819
* https://github.com/netty/netty/security/advisories/GHSA-93wv-jw9v-4972

## Security

* #214: Fixed vulnerability CVE-2026-59901 in dependency `io.netty:netty-codec:jar:4.1.135.Final:runtime`
* #215: Fixed vulnerability CVE-2026-59921 in dependency `io.netty:netty-codec-http:jar:4.1.135.Final:runtime`
* #216: Fixed vulnerability CVE-2026-56819 in dependency `io.netty:netty-codec-http2:jar:4.1.135.Final:runtime`

## Dependency Updates

### Compile Dependency Updates

* Updated `software.amazon.awssdk:dynamodb:2.46.9` to `2.48.3`

### Test Dependency Updates

* Updated `com.exasol:udf-debugging-java:0.6.18` to `0.6.20`
* Updated `nl.jqno.equalsverifier:equalsverifier:3.19.4` to `4.5`
* Updated `org.junit.jupiter:junit-jupiter-params:5.14.4` to `6.1.2`

### Plugin Dependency Updates

* Updated `com.exasol:project-keeper-maven-plugin:5.6.2` to `5.7.4`
