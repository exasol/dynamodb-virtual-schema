# Virtual Schema for Amazon DynamoDB 4.0.1, released 2026-06-12

Code name: Fixed vulnerability CVE-2026-42587 in io.netty:netty-codec-http:jar:4.1.133.Final:runtime

## Summary

This release fixes the following vulnerability:

### CVE-2026-42587 (CWE-400) in dependency `io.netty:netty-codec-http:jar:4.1.133.Final:runtime`
Netty is an asynchronous, event-driven network application framework. Prior to 4.2.13.Final and 4.1.133.Final, HttpContentDecompressor accepts a maxAllocation parameter to limit decompression buffer size and prevent decompression bomb attacks. This limit is correctly enforced for gzip and deflate encodings via ZlibDecoder, but is silently ignored when the content encoding is br (Brotli), zstd, or snappy. An attacker can bypass the configured decompression limit by sending a compressed payload with Content-Encoding: br instead of Content-Encoding: gzip, causing unbounded memory allocation and out-of-memory denial of service. The same vulnerability exists in DelegatingDecompressorFrameListener for HTTP/2 connections. This vulnerability is fixed in 4.2.13.Final and 4.1.133.Final.

Sonatype's research suggests that this CVE's details differ from those defined at NVD. See https://guide.sonatype.com/vulnerability/CVE-2026-42587 for details.

#### References
* https://guide.sonatype.com/vulnerability/CVE-2026-42587?component-type=maven&component-name=io.netty%2Fnetty-codec-http&utm_source=ossindex-client&utm_medium=integration&utm_content=1.8.1
* http://web.nvd.nist.gov/view/vuln/detail?vulnId=CVE-2026-42587
* https://github.com/advisories/GHSA-f6hv-jmp6-3vwv

## Security

* #208: Fixed vulnerability CVE-2026-42587 in dependency `io.netty:netty-codec-http:jar:4.1.133.Final:runtime`

## Dependency Updates

### Compile Dependency Updates

* Updated `software.amazon.awssdk:dynamodb:2.44.13` to `2.46.9`

### Test Dependency Updates

* Updated `com.exasol:test-db-builder-java:4.0.0` to `4.0.1`
