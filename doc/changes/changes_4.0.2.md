# Virtual Schema for Amazon DynamoDB 4.0.2, released 2026-07-28

Code name: Fixed vulnerabilities CVE-2026-55831, CVE-2026-55833, CVE-2026-56745, CVE-2026-56746, CVE-2026-59898, CVE-2026-59899, CVE-2026-59900, CVE-2026-59901, CVE-2026-59921, CVE-2026-56819, CVE-2026-9563, CVE-2026-54399, CVE-2026-54428

## Summary

This release fixes the following 13 vulnerabilities:

### CVE-2026-55831 (CWE-400) in dependency `io.netty:netty-codec-http:jar:4.1.135.Final:runtime`
Netty is a network application framework for development of protocol servers and clients. Prior to 4.1.136.Final and 4.2.16.Final, Netty's SPDY SETTINGS decoder accepts a peer-declared SETTINGS entry count up to the 24-bit frame-length limit and materializes every unique setting ID in `DefaultSpdySettingsFrame`, allowing a remote SPDY/3.1 peer to send a syntactically valid roughly 2 MiB SETTINGS frame that creates 262144 map entries and amplifies network input into heap growth and ordered-map insertion work. This issue is fixed in versions 4.1.136.Final and 4.2.16.Final.
#### References
* https://guide.sonatype.com/vulnerability/CVE-2026-55831?component-type=maven&component-name=io.netty%2Fnetty-codec-http&utm_source=ossindex-client&utm_medium=integration&utm_content=1.8.1
* http://web.nvd.nist.gov/view/vuln/detail?vulnId=CVE-2026-55831
* https://github.com/netty/netty/security/advisories/GHSA-6jqx-86gh-f27w

### CVE-2026-55833 (CWE-400) in dependency `io.netty:netty-codec-http:jar:4.1.135.Final:runtime`
Netty is a network application framework for development of protocol servers and clients. Prior to 4.1.136.Final and 4.2.16.Final, Netty SPDY header decoding continues inflating zlib-compressed header blocks after the raw header parser has exceeded `maxHeaderSize` and marked the frame truncated in `SpdyFrameCodec`, allowing a remote peer to send a small compressed `HEADERS` block that expands into much larger raw header data and causes compression-amplified CPU and allocation churn. This issue is fixed in versions 4.1.136.Final and 4.2.16.Final.
#### References
* https://guide.sonatype.com/vulnerability/CVE-2026-55833?component-type=maven&component-name=io.netty%2Fnetty-codec-http&utm_source=ossindex-client&utm_medium=integration&utm_content=1.8.1
* http://web.nvd.nist.gov/view/vuln/detail?vulnId=CVE-2026-55833
* https://github.com/netty/netty/releases/tag/netty-4.1.136.Final
* https://github.com/netty/netty/releases/tag/netty-4.2.16.Final
* https://github.com/netty/netty/security/advisories/GHSA-mvh2-crg5-v77c

### CVE-2026-56745 (CWE-400) in dependency `io.netty:netty-codec-http:jar:4.1.135.Final:runtime`
Netty is a network application framework for development of protocol servers and clients. In versions 4.2.0.Final through 4.2.15.Final and 4.1.0.Final through 4.1.135.Final, the `SpdyHttpDecoder` handler in Netty's SPDY-to-HTTP codec allocates a pooled `ByteBuf` when processing a client-initiated `SYN_STREAM` frame with `FLAG_FIN=0` and stores the partially constructed `FullHttpRequest` in `messageMap`; when the remote peer sends `RST_STREAM` for that stream or the accumulated content exceeds `maxContentLength`, the decoder removes the entry but does not release the pooled `ByteBuf`, causing native memory exhaustion. This issue is fixed in versions 4.1.136.Final and 4.2.16.Final.
#### References
* https://guide.sonatype.com/vulnerability/CVE-2026-56745?component-type=maven&component-name=io.netty%2Fnetty-codec-http&utm_source=ossindex-client&utm_medium=integration&utm_content=1.8.1
* http://web.nvd.nist.gov/view/vuln/detail?vulnId=CVE-2026-56745
* https://github.com/netty/netty/security/advisories/GHSA-jppx-w49h-x2qq

### CVE-2026-56746 (CWE-284) in dependency `io.netty:netty-codec-http:jar:4.1.135.Final:runtime`
Netty is a network application framework for development of protocol servers and clients. Versions 4.2.0.Final through 4.2.15.Final and 4.1.0.Final through 4.1.135.Final, are vulnerable to security control bypass during the origin evaluation process. CorsHandler provides a shortCircuit() configuration designed to reject unauthorized cross-origin requests immediately, acting as a security control before requests reach the application. However, due to a logical operator error in the origin evaluation process, this protection can be entirely bypassed. An attacker can bypass the short-circuit mechanism by sending a request with an Origin: null header. This failure forwards unauthorized requests to the backend application, bypassing intended access controls. This issue is fixed in versions 4.1.136.Final and 4.2.16.Final.
#### References
* https://guide.sonatype.com/vulnerability/CVE-2026-56746?component-type=maven&component-name=io.netty%2Fnetty-codec-http&utm_source=ossindex-client&utm_medium=integration&utm_content=1.8.1
* http://web.nvd.nist.gov/view/vuln/detail?vulnId=CVE-2026-56746
* https://github.com/netty/netty/security/advisories/GHSA-6cqp-g7gg-8hr5

### CVE-2026-59898 (CWE-444) in dependency `io.netty:netty-codec-http:jar:4.1.135.Final:runtime`
Netty - WebSocket handshaker missing header validation enables smuggling
#### References
* https://guide.sonatype.com/vulnerability/CVE-2026-59898?component-type=maven&component-name=io.netty%2Fnetty-codec-http&utm_source=ossindex-client&utm_medium=integration&utm_content=1.8.1
* http://web.nvd.nist.gov/view/vuln/detail?vulnId=CVE-2026-59898
* https://github.com/netty/netty/security/advisories/GHSA-4mp9-239f-g9hg

### CVE-2026-59899 (CWE-770) in dependency `io.netty:netty-codec-http:jar:4.1.135.Final:runtime`
io.netty/netty-codec-http - Unbounded queue growth via HTTP/1.1 pipelining leads to DoS
#### References
* https://guide.sonatype.com/vulnerability/CVE-2026-59899?component-type=maven&component-name=io.netty%2Fnetty-codec-http&utm_source=ossindex-client&utm_medium=integration&utm_content=1.8.1
* http://web.nvd.nist.gov/view/vuln/detail?vulnId=CVE-2026-59899
* https://github.com/netty/netty/releases/tag/netty-4.1.136.Final
* https://github.com/netty/netty/releases/tag/netty-4.2.16.Final
* https://github.com/netty/netty/security/advisories/GHSA-q4f6-jm68-57ww

### CVE-2026-59900 (CWE-444) in dependency `io.netty:netty-codec-http2:jar:4.1.135.Final:runtime`
Netty - HTTP/2 Host header deduplication failure enables request routing bypass
#### References
* https://guide.sonatype.com/vulnerability/CVE-2026-59900?component-type=maven&component-name=io.netty%2Fnetty-codec-http2&utm_source=ossindex-client&utm_medium=integration&utm_content=1.8.1
* http://web.nvd.nist.gov/view/vuln/detail?vulnId=CVE-2026-59900
* https://github.com/netty/netty/releases/tag/netty-4.1.136.Final
* https://github.com/netty/netty/releases/tag/netty-4.2.16.Final
* https://github.com/netty/netty/security/advisories/GHSA-c69g-56f8-xwqj


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


### CVE-2026-9563 (CWE-400) in dependency `org.eclipse.parsson:parsson:jar:1.1.7:test`
In Eclipse Parsson published Maven Central artifacts before version 1.1.8, the JSON parser did not enforce a default maximum on the number of characters consumed while parsing a single JSON document. Applications that parse attacker- controlled JSON can be forced to consume excessive CPU and memory by processing very large documents, including large arrays, objects, strings, numbers, whitespace, or nested structures, resulting in a denial of service. Eclipse Parsson 1.1.8 introduces a configurable maximum parsing limit with a default limit of 15 million parser-consumed characters.
#### References
* https://guide.sonatype.com/vulnerability/CVE-2026-9563?component-type=maven&component-name=org.eclipse.parsson%2Fparsson&utm_source=ossindex-client&utm_medium=integration&utm_content=1.8.1
* http://web.nvd.nist.gov/view/vuln/detail?vulnId=CVE-2026-9563
* https://github.com/eclipse-ee4j/parsson/pull/169
* https://gitlab.eclipse.org/security/vulnerability-reports/-/work_items/444

### CVE-2026-54399 (CWE-400) in dependency `org.apache.httpcomponents.core5:httpcore5:jar:5.4.2:runtime`
Uncontrolled Resource Consumption vulnerability in the HTTP/1.1 message parserÂ in Apache HttpComponents Core (5.4.2 and earlier, 5.5-beta1 and earlier) allowsÂ an remote attacker to cause a denial of service through memory exhaustion by sending messages with excessive number of headers / excessive header length

Sonatype's research suggests that this CVE's details differ from those defined at NVD. See https://guide.sonatype.com/vulnerability/CVE-2026-54399 for details
#### References
* https://guide.sonatype.com/vulnerability/CVE-2026-54399?component-type=maven&component-name=org.apache.httpcomponents.core5%2Fhttpcore5&utm_source=ossindex-client&utm_medium=integration&utm_content=1.8.1
* http://web.nvd.nist.gov/view/vuln/detail?vulnId=CVE-2026-54399
* https://lists.apache.org/thread/zmxh1pl2zohov5ntdh4lt85gfrlchgpy
* http://www.openwall.com/lists/oss-security/2026/07/01/4

### CVE-2026-54428 (CWE-400) in dependency `org.apache.httpcomponents.core5:httpcore5-h2:jar:5.4:runtime`
Allocation of resources without limits or throttling in the HTTP/2 HPACK decoder in Apache HttpComponents Core (5.4.2 and earlier, 5.5-beta1 and earlier) allows an remote attacker to cause a denial of service through memory exhaustion by sending oversized compressed header blocks before the HTTP/2 SETTINGS acknowledgement causes the configured header list size limit to be applied.
#### References
* https://guide.sonatype.com/vulnerability/CVE-2026-54428?component-type=maven&component-name=org.apache.httpcomponents.core5%2Fhttpcore5-h2&utm_source=ossindex-client&utm_medium=integration&utm_content=1.8.1
* http://web.nvd.nist.gov/view/vuln/detail?vulnId=CVE-2026-54428
* https://lists.apache.org/thread/5zjp8vczvxq19pw2rvhs21q446bhl0sd

## Security

* #210: Fixed vulnerability CVE-2026-9563 in dependency `org.eclipse.parsson:parsson:jar:1.1.7:test`
* #211: Fixed vulnerability CVE-2026-54399 in dependency `org.apache.httpcomponents.core5:httpcore5:jar:5.4.2:runtime`
* #212: Fixed vulnerability CVE-2026-54428 in dependency `org.apache.httpcomponents.core5:httpcore5-h2:jar:5.4:runtime`
* #214: Fixed vulnerability CVE-2026-59901 in dependency `io.netty:netty-codec:jar:4.1.135.Final:runtime`
* #215: Fixed vulnerability CVE-2026-59921 in dependency `io.netty:netty-codec-http:jar:4.1.135.Final:runtime`
* #216: Fixed vulnerability CVE-2026-56819 in dependency `io.netty:netty-codec-http2:jar:4.1.135.Final:runtime`
* #218: Fixed vulnerability CVE-2026-55831 in dependency `io.netty:netty-codec-http:jar:4.1.135.Final:runtime`
* #219: Fixed vulnerability CVE-2026-55833 in dependency `io.netty:netty-codec-http:jar:4.1.135.Final:runtime`
* #220: Fixed vulnerability CVE-2026-56745 in dependency `io.netty:netty-codec-http:jar:4.1.135.Final:runtime`
* #221: Fixed vulnerability CVE-2026-56746 in dependency `io.netty:netty-codec-http:jar:4.1.135.Final:runtime`
* #222: Fixed vulnerability CVE-2026-59898 in dependency `io.netty:netty-codec-http:jar:4.1.135.Final:runtime`
* #223: Fixed vulnerability CVE-2026-59899 in dependency `io.netty:netty-codec-http:jar:4.1.135.Final:runtime`
* #224: Fixed vulnerability CVE-2026-59900 in dependency `io.netty:netty-codec-http2:jar:4.1.135.Final:runtime`

## Dependency Updates

### Compile Dependency Updates

* Updated `com.exasol:virtual-schema-common-document:12.0.2` to `12.0.3`
* Updated `software.amazon.awssdk:dynamodb:2.46.9` to `2.49.4`

### Test Dependency Updates

* Updated `com.exasol:exasol-testcontainers:7.3.0` to `8.0.1`
* Updated `com.exasol:hamcrest-resultset-matcher:1.7.2` to `1.7.3`
* Updated `com.exasol:udf-debugging-java:0.6.18` to `0.6.20`
* Updated `com.exasol:virtual-schema-common-document:12.0.2` to `12.0.3`
* Updated `org.jacoco:org.jacoco.agent:0.8.14` to `0.8.15`

### Plugin Dependency Updates

* Updated `com.exasol:artifact-reference-checker-maven-plugin:0.4.4` to `1.0.1`
* Updated `com.exasol:error-code-crawler-maven-plugin:2.0.7` to `2.1.0`
* Updated `com.exasol:project-keeper-maven-plugin:5.6.2` to `5.7.4`
* Removed `com.exasol:quality-summarizer-maven-plugin:0.2.1`
* Updated `org.apache.maven.plugins:maven-dependency-plugin:3.10.0` to `3.11.0`
* Updated `org.apache.maven.plugins:maven-enforcer-plugin:3.6.2` to `3.6.3`
* Updated `org.apache.maven.plugins:maven-failsafe-plugin:3.5.5` to `3.5.6`
* Updated `org.apache.maven.plugins:maven-site-plugin:3.21.0` to `3.22.0`
* Updated `org.apache.maven.plugins:maven-surefire-plugin:3.5.5` to `3.5.6`
* Updated `org.jacoco:jacoco-maven-plugin:0.8.14` to `0.8.15`
* Updated `org.sonarsource.scanner.maven:sonar-maven-plugin:5.5.0.6356` to `5.7.0.6970`
* Added `org.spdx:spdx-maven-plugin:1.0.4`
