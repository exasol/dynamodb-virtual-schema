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
| [JUnit Jupiter Engine][8]                       | [Eclipse Public License v2.0][9]  |
| [JUnit Jupiter Params][8]                       | [Eclipse Public License v2.0][9]  |
| [mockito-junit-jupiter][10]                     | [The MIT License][11]             |
| [Common Virtual Schema for document data][2]    | [MIT License][3]                  |
| [Test containers for Exasol on Docker][12]      | [MIT License][13]                 |
| [Testcontainers :: JUnit Jupiter Extension][14] | [MIT][15]                         |
| [Test Database Builder for Java][16]            | [MIT License][17]                 |
| [udf-debugging-java][18]                        | [MIT License][19]                 |
| [Matcher for SQL Result Sets][20]               | [MIT License][21]                 |
| [EqualsVerifier | release normal jar][22]       | [Apache License, Version 2.0][23] |
| [JaCoCo :: Agent][24]                           | [Eclipse Public License 2.0][25]  |

## Runtime Dependencies

| Dependency                | License           |
| ------------------------- | ----------------- |
| [SLF4J JDK14 Binding][26] | [MIT License][27] |

## Plugin Dependencies

| Dependency                                              | License                                        |
| ------------------------------------------------------- | ---------------------------------------------- |
| [SonarQube Scanner for Maven][28]                       | [GNU LGPL 3][29]                               |
| [Apache Maven Compiler Plugin][30]                      | [Apache-2.0][23]                               |
| [Apache Maven Enforcer Plugin][31]                      | [Apache-2.0][23]                               |
| [Maven Flatten Plugin][32]                              | [Apache Software Licenese][23]                 |
| [org.sonatype.ossindex.maven:ossindex-maven-plugin][33] | [ASL2][34]                                     |
| [Maven Surefire Plugin][35]                             | [Apache-2.0][23]                               |
| [Versions Maven Plugin][36]                             | [Apache License, Version 2.0][23]              |
| [duplicate-finder-maven-plugin Maven Mojo][37]          | [Apache License 2.0][38]                       |
| [Apache Maven Assembly Plugin][39]                      | [Apache License, Version 2.0][23]              |
| [Apache Maven JAR Plugin][40]                           | [Apache License, Version 2.0][23]              |
| [Artifact reference checker and unifier][41]            | [MIT License][42]                              |
| [Apache Maven Dependency Plugin][43]                    | [Apache License, Version 2.0][23]              |
| [Project keeper maven plugin][44]                       | [The MIT License][45]                          |
| [Maven Failsafe Plugin][46]                             | [Apache-2.0][23]                               |
| [JaCoCo :: Maven Plugin][47]                            | [Eclipse Public License 2.0][25]               |
| [error-code-crawler-maven-plugin][48]                   | [MIT License][49]                              |
| [Reproducible Build Maven Plugin][50]                   | [Apache 2.0][34]                               |
| [Maven Clean Plugin][51]                                | [The Apache Software License, Version 2.0][34] |
| [Maven Resources Plugin][52]                            | [The Apache Software License, Version 2.0][34] |
| [Maven Install Plugin][53]                              | [The Apache Software License, Version 2.0][34] |
| [Maven Deploy Plugin][54]                               | [The Apache Software License, Version 2.0][34] |
| [Maven Site Plugin 3][55]                               | [The Apache Software License, Version 2.0][34] |

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
[11]: https://github.com/mockito/mockito/blob/main/LICENSE
[12]: https://github.com/exasol/exasol-testcontainers/
[13]: https://github.com/exasol/exasol-testcontainers/blob/main/LICENSE
[14]: https://testcontainers.org
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
[30]: https://maven.apache.org/plugins/maven-compiler-plugin/
[31]: https://maven.apache.org/enforcer/maven-enforcer-plugin/
[32]: https://www.mojohaus.org/flatten-maven-plugin/
[33]: https://sonatype.github.io/ossindex-maven/maven-plugin/
[34]: http://www.apache.org/licenses/LICENSE-2.0.txt
[35]: https://maven.apache.org/surefire/maven-surefire-plugin/
[36]: https://www.mojohaus.org/versions/versions-maven-plugin/
[37]: https://github.com/basepom/duplicate-finder-maven-plugin
[38]: http://www.apache.org/licenses/LICENSE-2.0.html
[39]: https://maven.apache.org/plugins/maven-assembly-plugin/
[40]: https://maven.apache.org/plugins/maven-jar-plugin/
[41]: https://github.com/exasol/artifact-reference-checker-maven-plugin/
[42]: https://github.com/exasol/artifact-reference-checker-maven-plugin/blob/main/LICENSE
[43]: https://maven.apache.org/plugins/maven-dependency-plugin/
[44]: https://github.com/exasol/project-keeper/
[45]: https://github.com/exasol/project-keeper/blob/main/LICENSE
[46]: https://maven.apache.org/surefire/maven-failsafe-plugin/
[47]: https://www.jacoco.org/jacoco/trunk/doc/maven.html
[48]: https://github.com/exasol/error-code-crawler-maven-plugin/
[49]: https://github.com/exasol/error-code-crawler-maven-plugin/blob/main/LICENSE
[50]: http://zlika.github.io/reproducible-build-maven-plugin
[51]: http://maven.apache.org/plugins/maven-clean-plugin/
[52]: http://maven.apache.org/plugins/maven-resources-plugin/
[53]: http://maven.apache.org/plugins/maven-install-plugin/
[54]: http://maven.apache.org/plugins/maven-deploy-plugin/
[55]: http://maven.apache.org/plugins/maven-site-plugin/
