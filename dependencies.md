<!-- @formatter:off -->
# Dependencies

## Compile Dependencies

| Dependency                                       | License                          |
| ------------------------------------------------ | -------------------------------- |
| [error-reporting-java][0]                        | [MIT License][1]                 |
| [Common Virtual Schema for document data][2]     | [MIT License][3]                 |
| [AWS Java SDK :: Services :: Amazon DynamoDB][4] | [Apache License, Version 2.0][5] |

## Test Dependencies

| Dependency                                      | License                           |
| ----------------------------------------------- | --------------------------------- |
| [Hamcrest][6]                                   | [BSD License 3][7]                |
| [JUnit Jupiter Params][8]                       | [Eclipse Public License v2.0][9]  |
| [mockito-junit-jupiter][10]                     | [MIT][11]                         |
| [Common Virtual Schema for document data][2]    | [MIT License][3]                  |
| [Test containers for Exasol on Docker][12]      | [MIT License][13]                 |
| [Testcontainers :: JUnit Jupiter Extension][14] | [MIT][15]                         |
| [Test Database Builder for Java][16]            | [MIT License][17]                 |
| [udf-debugging-java][18]                        | [MIT License][19]                 |
| [Matcher for SQL Result Sets][20]               | [MIT License][21]                 |
| [EqualsVerifier \| release normal jar][22]      | [Apache License, Version 2.0][23] |
| [JaCoCo :: Agent][24]                           | [Eclipse Public License 2.0][25]  |

## Runtime Dependencies

| Dependency                 | License           |
| -------------------------- | ----------------- |
| [SLF4J JDK14 Provider][26] | [MIT License][27] |

## Plugin Dependencies

| Dependency                                              | License                           |
| ------------------------------------------------------- | --------------------------------- |
| [SonarQube Scanner for Maven][28]                       | [GNU LGPL 3][29]                  |
| [Apache Maven Toolchains Plugin][30]                    | [Apache License, Version 2.0][23] |
| [Apache Maven Compiler Plugin][31]                      | [Apache-2.0][23]                  |
| [Apache Maven Enforcer Plugin][32]                      | [Apache-2.0][23]                  |
| [Maven Flatten Plugin][33]                              | [Apache Software Licenese][23]    |
| [org.sonatype.ossindex.maven:ossindex-maven-plugin][34] | [ASL2][35]                        |
| [Maven Surefire Plugin][36]                             | [Apache-2.0][23]                  |
| [Versions Maven Plugin][37]                             | [Apache License, Version 2.0][23] |
| [duplicate-finder-maven-plugin Maven Mojo][38]          | [Apache License 2.0][39]          |
| [Apache Maven Assembly Plugin][40]                      | [Apache-2.0][23]                  |
| [Apache Maven JAR Plugin][41]                           | [Apache License, Version 2.0][23] |
| [Artifact reference checker and unifier][42]            | [MIT License][43]                 |
| [Apache Maven Dependency Plugin][44]                    | [Apache-2.0][23]                  |
| [Project Keeper Maven plugin][45]                       | [The MIT License][46]             |
| [Maven Failsafe Plugin][47]                             | [Apache-2.0][23]                  |
| [JaCoCo :: Maven Plugin][48]                            | [Eclipse Public License 2.0][25]  |
| [error-code-crawler-maven-plugin][49]                   | [MIT License][50]                 |
| [Reproducible Build Maven Plugin][51]                   | [Apache 2.0][35]                  |

[0]: https://github.com/exasol/error-reporting-java/
[1]: https://github.com/exasol/error-reporting-java/blob/main/LICENSE
[2]: https://github.com/exasol/virtual-schema-common-document/
[3]: https://github.com/exasol/virtual-schema-common-document/blob/main/LICENSE
[4]: https://aws.amazon.com/sdkforjava
[5]: https://aws.amazon.com/apache2.0
[6]: http://hamcrest.org/JavaHamcrest/
[7]: http://opensource.org/licenses/BSD-3-Clause
[8]: https://junit.org/junit5/
[9]: https://www.eclipse.org/legal/epl-v20.html
[10]: https://github.com/mockito/mockito
[11]: https://opensource.org/licenses/MIT
[12]: https://github.com/exasol/exasol-testcontainers/
[13]: https://github.com/exasol/exasol-testcontainers/blob/main/LICENSE
[14]: https://java.testcontainers.org
[15]: http://opensource.org/licenses/MIT
[16]: https://github.com/exasol/test-db-builder-java/
[17]: https://github.com/exasol/test-db-builder-java/blob/main/LICENSE
[18]: https://github.com/exasol/udf-debugging-java/
[19]: https://github.com/exasol/udf-debugging-java/blob/main/LICENSE
[20]: https://github.com/exasol/hamcrest-resultset-matcher/
[21]: https://github.com/exasol/hamcrest-resultset-matcher/blob/main/LICENSE
[22]: https://www.jqno.nl/equalsverifier
[23]: https://www.apache.org/licenses/LICENSE-2.0.txt
[24]: https://www.eclemma.org/jacoco/index.html
[25]: https://www.eclipse.org/legal/epl-2.0/
[26]: http://www.slf4j.org
[27]: http://www.opensource.org/licenses/mit-license.php
[28]: http://sonarsource.github.io/sonar-scanner-maven/
[29]: http://www.gnu.org/licenses/lgpl.txt
[30]: https://maven.apache.org/plugins/maven-toolchains-plugin/
[31]: https://maven.apache.org/plugins/maven-compiler-plugin/
[32]: https://maven.apache.org/enforcer/maven-enforcer-plugin/
[33]: https://www.mojohaus.org/flatten-maven-plugin/
[34]: https://sonatype.github.io/ossindex-maven/maven-plugin/
[35]: http://www.apache.org/licenses/LICENSE-2.0.txt
[36]: https://maven.apache.org/surefire/maven-surefire-plugin/
[37]: https://www.mojohaus.org/versions/versions-maven-plugin/
[38]: https://basepom.github.io/duplicate-finder-maven-plugin
[39]: http://www.apache.org/licenses/LICENSE-2.0.html
[40]: https://maven.apache.org/plugins/maven-assembly-plugin/
[41]: https://maven.apache.org/plugins/maven-jar-plugin/
[42]: https://github.com/exasol/artifact-reference-checker-maven-plugin/
[43]: https://github.com/exasol/artifact-reference-checker-maven-plugin/blob/main/LICENSE
[44]: https://maven.apache.org/plugins/maven-dependency-plugin/
[45]: https://github.com/exasol/project-keeper/
[46]: https://github.com/exasol/project-keeper/blob/main/LICENSE
[47]: https://maven.apache.org/surefire/maven-failsafe-plugin/
[48]: https://www.jacoco.org/jacoco/trunk/doc/maven.html
[49]: https://github.com/exasol/error-code-crawler-maven-plugin/
[50]: https://github.com/exasol/error-code-crawler-maven-plugin/blob/main/LICENSE
[51]: http://zlika.github.io/reproducible-build-maven-plugin
