<!-- @formatter:off -->
# Dependencies

## Compile Dependencies

| Dependency                                       | License                          |
| ------------------------------------------------ | -------------------------------- |
| [error-reporting-java][0]                        | [MIT License][1]                 |
| [Common Virtual Schema for document data][2]     | [MIT License][3]                 |
| [AWS Java SDK :: Services :: Amazon DynamoDB][4] | [Apache License, Version 2.0][5] |
| [Matcher for SQL Result Sets][6]                 | [MIT License][7]                 |
| [Project Lombok][8]                              | [The MIT License][9]             |

## Test Dependencies

| Dependency                                      | License                           |
| ----------------------------------------------- | --------------------------------- |
| [Hamcrest][10]                                  | [BSD License 3][11]               |
| [JUnit Jupiter Engine][12]                      | [Eclipse Public License v2.0][13] |
| [JUnit Jupiter Params][12]                      | [Eclipse Public License v2.0][13] |
| [mockito-junit-jupiter][14]                     | [The MIT License][15]             |
| [Common Virtual Schema for document data][2]    | [MIT License][3]                  |
| [Test containers for Exasol on Docker][16]      | [MIT License][17]                 |
| [Testcontainers :: JUnit Jupiter Extension][18] | [MIT][19]                         |
| [Test Database Builder for Java][20]            | [MIT License][21]                 |
| [udf-debugging-java][22]                        | [MIT License][23]                 |
| [JaCoCo :: Agent][24]                           | [Eclipse Public License 2.0][25]  |

## Runtime Dependencies

| Dependency                | License           |
| ------------------------- | ----------------- |
| [SLF4J JDK14 Binding][26] | [MIT License][27] |

## Plugin Dependencies

| Dependency                                              | License                                        |
| ------------------------------------------------------- | ---------------------------------------------- |
| [SonarQube Scanner for Maven][28]                       | [GNU LGPL 3][29]                               |
| [Apache Maven Compiler Plugin][30]                      | [Apache-2.0][31]                               |
| [Apache Maven Enforcer Plugin][32]                      | [Apache-2.0][31]                               |
| [Maven Flatten Plugin][33]                              | [Apache Software Licenese][31]                 |
| [org.sonatype.ossindex.maven:ossindex-maven-plugin][34] | [ASL2][35]                                     |
| [Maven Surefire Plugin][36]                             | [Apache-2.0][31]                               |
| [Versions Maven Plugin][37]                             | [Apache License, Version 2.0][31]              |
| [duplicate-finder-maven-plugin Maven Mojo][38]          | [Apache License 2.0][39]                       |
| [Apache Maven Assembly Plugin][40]                      | [Apache License, Version 2.0][31]              |
| [Apache Maven JAR Plugin][41]                           | [Apache License, Version 2.0][31]              |
| [Artifact reference checker and unifier][42]            | [MIT License][43]                              |
| [Apache Maven Dependency Plugin][44]                    | [Apache License, Version 2.0][31]              |
| [Lombok Maven Plugin][45]                               | [The MIT License][46]                          |
| [Project keeper maven plugin][47]                       | [The MIT License][48]                          |
| [Maven Failsafe Plugin][49]                             | [Apache-2.0][31]                               |
| [JaCoCo :: Maven Plugin][50]                            | [Eclipse Public License 2.0][25]               |
| [error-code-crawler-maven-plugin][51]                   | [MIT License][52]                              |
| [Reproducible Build Maven Plugin][53]                   | [Apache 2.0][35]                               |
| [Maven Clean Plugin][54]                                | [The Apache Software License, Version 2.0][35] |
| [Maven Resources Plugin][55]                            | [The Apache Software License, Version 2.0][35] |
| [Maven Install Plugin][56]                              | [The Apache Software License, Version 2.0][35] |
| [Maven Deploy Plugin][57]                               | [The Apache Software License, Version 2.0][35] |
| [Maven Site Plugin 3][58]                               | [The Apache Software License, Version 2.0][35] |

[0]: https://github.com/exasol/error-reporting-java/
[1]: https://github.com/exasol/error-reporting-java/blob/main/LICENSE
[2]: https://github.com/exasol/virtual-schema-common-document/
[3]: https://github.com/exasol/virtual-schema-common-document/blob/main/LICENSE
[4]: https://aws.amazon.com/sdkforjava
[5]: https://aws.amazon.com/apache2.0
[6]: https://github.com/exasol/hamcrest-resultset-matcher/
[7]: https://github.com/exasol/hamcrest-resultset-matcher/blob/main/LICENSE
[8]: https://projectlombok.org
[9]: https://projectlombok.org/LICENSE
[10]: http://hamcrest.org/JavaHamcrest/
[11]: http://opensource.org/licenses/BSD-3-Clause
[12]: https://junit.org/junit5/
[13]: https://www.eclipse.org/legal/epl-v20.html
[14]: https://github.com/mockito/mockito
[15]: https://github.com/mockito/mockito/blob/main/LICENSE
[16]: https://github.com/exasol/exasol-testcontainers/
[17]: https://github.com/exasol/exasol-testcontainers/blob/main/LICENSE
[18]: https://testcontainers.org
[19]: http://opensource.org/licenses/MIT
[20]: https://github.com/exasol/test-db-builder-java/
[21]: https://github.com/exasol/test-db-builder-java/blob/main/LICENSE
[22]: https://github.com/exasol/udf-debugging-java/
[23]: https://github.com/exasol/udf-debugging-java/blob/main/LICENSE
[24]: https://www.eclemma.org/jacoco/index.html
[25]: https://www.eclipse.org/legal/epl-2.0/
[26]: http://www.slf4j.org
[27]: http://www.opensource.org/licenses/mit-license.php
[28]: http://sonarsource.github.io/sonar-scanner-maven/
[29]: http://www.gnu.org/licenses/lgpl.txt
[30]: https://maven.apache.org/plugins/maven-compiler-plugin/
[31]: https://www.apache.org/licenses/LICENSE-2.0.txt
[32]: https://maven.apache.org/enforcer/maven-enforcer-plugin/
[33]: https://www.mojohaus.org/flatten-maven-plugin/
[34]: https://sonatype.github.io/ossindex-maven/maven-plugin/
[35]: http://www.apache.org/licenses/LICENSE-2.0.txt
[36]: https://maven.apache.org/surefire/maven-surefire-plugin/
[37]: https://www.mojohaus.org/versions/versions-maven-plugin/
[38]: https://github.com/basepom/duplicate-finder-maven-plugin
[39]: http://www.apache.org/licenses/LICENSE-2.0.html
[40]: https://maven.apache.org/plugins/maven-assembly-plugin/
[41]: https://maven.apache.org/plugins/maven-jar-plugin/
[42]: https://github.com/exasol/artifact-reference-checker-maven-plugin/
[43]: https://github.com/exasol/artifact-reference-checker-maven-plugin/blob/main/LICENSE
[44]: https://maven.apache.org/plugins/maven-dependency-plugin/
[45]: https://anthonywhitford.com/lombok.maven/lombok-maven-plugin/
[46]: https://opensource.org/licenses/MIT
[47]: https://github.com/exasol/project-keeper/
[48]: https://github.com/exasol/project-keeper/blob/main/LICENSE
[49]: https://maven.apache.org/surefire/maven-failsafe-plugin/
[50]: https://www.jacoco.org/jacoco/trunk/doc/maven.html
[51]: https://github.com/exasol/error-code-crawler-maven-plugin/
[52]: https://github.com/exasol/error-code-crawler-maven-plugin/blob/main/LICENSE
[53]: http://zlika.github.io/reproducible-build-maven-plugin
[54]: http://maven.apache.org/plugins/maven-clean-plugin/
[55]: http://maven.apache.org/plugins/maven-resources-plugin/
[56]: http://maven.apache.org/plugins/maven-install-plugin/
[57]: http://maven.apache.org/plugins/maven-deploy-plugin/
[58]: http://maven.apache.org/plugins/maven-site-plugin/
