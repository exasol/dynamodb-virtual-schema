# Virtual Schema for Amazon DynamoDB 4.0.4, released 2026-??-??

Code name: Fixed vulnerability CVE-2026-116486 in org.objenesis:objenesis:jar:3.4:test

## Summary

This release fixes the following vulnerability:

### CVE-2026-116486 (CWE-386) in dependency `org.objenesis:objenesis:jar:3.4:test`
Objenesis before 3.6 contains a type confusion vulnerability that allows code in one class loader to substitute objects into another by exploiting instantiator caching keyed on class name. When a shared ObjenesisStd instance caches instantiators by clazz.getName() in ObjenesisBase.getInstantiatorOf(), same-named classes collide, causing newInstance() to return wrong-class objects and ClassCastException failures.
#### References
* https://guide.sonatype.com/vulnerability/CVE-2026-116486?component-type=maven&component-name=org.objenesis%2Fobjenesis&utm_source=ossindex-client&utm_medium=integration&utm_content=1.8.1
* http://web.nvd.nist.gov/view/vuln/detail?vulnId=CVE-2026-116486
* https://github.com/CVEProject/cvelistV5/blob/main/cves/2026/116xxx/CVE-2026-116486.json

## Security

* #267: Fixed vulnerability CVE-2026-116486 in dependency `org.objenesis:objenesis:jar:3.4:test`

## Dependency Updates

### Compile Dependency Updates

* Updated `software.amazon.awssdk:dynamodb:2.57.3` to `2.57.8`

### Test Dependency Updates

* Updated `nl.jqno.equalsverifier:equalsverifier:3.19.4` to `4.5.2`
* Updated `org.junit.jupiter:junit-jupiter-params:5.14.4` to `6.1.3`

### Plugin Dependency Updates

* Updated `com.exasol:project-keeper-maven-plugin:5.8.1` to `5.8.2`
