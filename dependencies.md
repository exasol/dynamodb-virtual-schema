<!-- @formatter:off -->
# Dependencies

## Compile Dependencies

| Dependency                                       | License                          |
| ------------------------------------------------ | -------------------------------- |
| [error-reporting-java][0]                        | [MIT][1]                         |
| [Common Virtual Schema for document data][2]     | [MIT][1]                         |
| [AWS Java SDK :: Services :: Amazon DynamoDB][4] | [Apache License, Version 2.0][5] |
| [SLF4J JDK14 Binding][6]                         | [MIT License][7]                 |
| [Matcher for SQL Result Sets][8]                 | [MIT][1]                         |

## Test Dependencies

| Dependency                                      | License                           |
| ----------------------------------------------- | --------------------------------- |
| [Hamcrest][10]                                  | [BSD License 3][11]               |
| [JUnit Jupiter Engine][12]                      | [Eclipse Public License v2.0][13] |
| [JUnit Jupiter Params][12]                      | [Eclipse Public License v2.0][13] |
| [mockito-junit-jupiter][16]                     | [The MIT License][17]             |
| [Common Virtual Schema for document data][2]    | [MIT][1]                          |
| [Test containers for Exasol on Docker][20]      | [MIT][1]                          |
| [Testcontainers :: JUnit Jupiter Extension][22] | [MIT][23]                         |
| [JaCoCo :: Core][24]                            | [Eclipse Public License 2.0][25]  |
| [Test Database Builder for Java][26]            | [MIT][1]                          |
| [udf-debugging-java][28]                        | [MIT][1]                          |

## Runtime Dependencies

| Dependency            | License                          |
| --------------------- | -------------------------------- |
| [JaCoCo :: Agent][24] | [Eclipse Public License 2.0][25] |

## Plugin Dependencies

| Dependency                                              | License                                        |
| ------------------------------------------------------- | ---------------------------------------------- |
| [Maven Surefire Plugin][32]                             | [Apache License, Version 2.0][33]              |
| [Maven Failsafe Plugin][34]                             | [Apache License, Version 2.0][33]              |
| [JaCoCo :: Maven Plugin][36]                            | [Eclipse Public License 2.0][25]               |
| [Apache Maven Assembly Plugin][38]                      | [Apache License, Version 2.0][33]              |
| [Apache Maven Compiler Plugin][40]                      | [Apache License, Version 2.0][33]              |
| [Maven Dependency Plugin][42]                           | [The Apache Software License, Version 2.0][43] |
| [Versions Maven Plugin][44]                             | [Apache License, Version 2.0][33]              |
| [org.sonatype.ossindex.maven:ossindex-maven-plugin][46] | [ASL2][43]                                     |
| [Apache Maven Enforcer Plugin][48]                      | [Apache License, Version 2.0][33]              |
| [Artifact reference checker and unifier][50]            | [MIT][1]                                       |
| [Project keeper maven plugin][52]                       | [MIT][1]                                       |
| [error-code-crawler-maven-plugin][54]                   | [MIT][1]                                       |
| [Reproducible Build Maven Plugin][56]                   | [Apache 2.0][43]                               |
| [Apache Maven JAR Plugin][58]                           | [Apache License, Version 2.0][33]              |
| [Maven Clean Plugin][60]                                | [The Apache Software License, Version 2.0][43] |
| [Maven Resources Plugin][62]                            | [The Apache Software License, Version 2.0][43] |
| [Maven Install Plugin][64]                              | [The Apache Software License, Version 2.0][43] |
| [Maven Deploy Plugin][66]                               | [The Apache Software License, Version 2.0][43] |
| [Maven Site Plugin 3][68]                               | [The Apache Software License, Version 2.0][43] |

[2]: https://github.com/exasol/virtual-schema-common-document
[24]: https://www.eclemma.org/jacoco/index.html
[52]: https://github.com/exasol/project-keeper-maven-plugin
[0]: https://github.com/exasol/error-reporting-java
[43]: http://www.apache.org/licenses/LICENSE-2.0.txt
[32]: https://maven.apache.org/surefire/maven-surefire-plugin/
[60]: http://maven.apache.org/plugins/maven-clean-plugin/
[4]: https://aws.amazon.com/sdkforjava
[1]: https://opensource.org/licenses/MIT
[16]: https://github.com/mockito/mockito
[34]: https://maven.apache.org/surefire/maven-failsafe-plugin/
[26]: https://github.com/exasol/test-db-builder-java
[42]: http://maven.apache.org/plugins/maven-dependency-plugin/
[44]: http://www.mojohaus.org/versions-maven-plugin/
[11]: http://opensource.org/licenses/BSD-3-Clause
[40]: https://maven.apache.org/plugins/maven-compiler-plugin/
[23]: http://opensource.org/licenses/MIT
[25]: https://www.eclipse.org/legal/epl-2.0/
[20]: https://github.com/exasol/exasol-testcontainers
[36]: https://www.jacoco.org/jacoco/trunk/doc/maven.html
[5]: https://aws.amazon.com/apache2.0
[17]: https://github.com/mockito/mockito/blob/main/LICENSE
[8]: https://github.com/exasol/hamcrest-resultset-matcher
[56]: http://zlika.github.io/reproducible-build-maven-plugin
[7]: http://www.opensource.org/licenses/mit-license.php
[33]: https://www.apache.org/licenses/LICENSE-2.0.txt
[48]: https://maven.apache.org/enforcer/maven-enforcer-plugin/
[13]: https://www.eclipse.org/legal/epl-v20.html
[64]: http://maven.apache.org/plugins/maven-install-plugin/
[12]: https://junit.org/junit5/
[46]: https://sonatype.github.io/ossindex-maven/maven-plugin/
[22]: https://testcontainers.org
[28]: https://github.com/exasol/udf-debugging-java
[10]: http://hamcrest.org/JavaHamcrest/
[6]: http://www.slf4j.org
[66]: http://maven.apache.org/plugins/maven-deploy-plugin/
[68]: http://maven.apache.org/plugins/maven-site-plugin/
[62]: http://maven.apache.org/plugins/maven-resources-plugin/
[50]: https://github.com/exasol/artifact-reference-checker-maven-plugin
[54]: https://github.com/exasol/error-code-crawler-maven-plugin
[58]: https://maven.apache.org/plugins/maven-jar-plugin/
[38]: https://maven.apache.org/plugins/maven-assembly-plugin/
