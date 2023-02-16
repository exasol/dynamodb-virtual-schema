<!-- @formatter:off -->
# Dependencies

## Compile Dependencies

| Dependency                                       | License                          |
| ------------------------------------------------ | -------------------------------- |
| [error-reporting-java][0]                        | [MIT License][1]                 |
| [Common Virtual Schema for document data][2]     | [MIT][3]                         |
| [AWS Java SDK :: Services :: Amazon DynamoDB][4] | [Apache License, Version 2.0][5] |
| [SLF4J JDK14 Binding][6]                         | [MIT License][7]                 |
| [Matcher for SQL Result Sets][8]                 | [MIT License][9]                 |
| [Project Lombok][10]                             | [The MIT License][11]            |

## Test Dependencies

| Dependency                                      | License                           |
| ----------------------------------------------- | --------------------------------- |
| [Hamcrest][12]                                  | [BSD License 3][13]               |
| [JUnit Jupiter Engine][14]                      | [Eclipse Public License v2.0][15] |
| [JUnit Jupiter Params][14]                      | [Eclipse Public License v2.0][15] |
| [mockito-junit-jupiter][16]                     | [The MIT License][17]             |
| [Common Virtual Schema for document data][2]    | [MIT][3]                          |
| [Test containers for Exasol on Docker][18]      | [MIT License][19]                 |
| [Testcontainers :: JUnit Jupiter Extension][20] | [MIT][21]                         |
| [JaCoCo :: Agent][22]                           | [Eclipse Public License 2.0][23]  |
| [JaCoCo :: Core][22]                            | [Eclipse Public License 2.0][23]  |
| [Test Database Builder for Java][24]            | [MIT License][25]                 |
| [udf-debugging-java][26]                        | [MIT License][27]                 |

## Plugin Dependencies

| Dependency                                              | License                                        |
| ------------------------------------------------------- | ---------------------------------------------- |
| [SonarQube Scanner for Maven][28]                       | [GNU LGPL 3][29]                               |
| [Apache Maven Compiler Plugin][30]                      | [Apache License, Version 2.0][31]              |
| [Apache Maven Enforcer Plugin][32]                      | [Apache License, Version 2.0][31]              |
| [Maven Flatten Plugin][33]                              | [Apache Software Licenese][31]                 |
| [org.sonatype.ossindex.maven:ossindex-maven-plugin][34] | [ASL2][35]                                     |
| [Maven Surefire Plugin][36]                             | [Apache License, Version 2.0][31]              |
| [Versions Maven Plugin][37]                             | [Apache License, Version 2.0][31]              |
| [Apache Maven Assembly Plugin][38]                      | [Apache License, Version 2.0][31]              |
| [Apache Maven JAR Plugin][39]                           | [Apache License, Version 2.0][31]              |
| [Artifact reference checker and unifier][40]            | [MIT License][41]                              |
| [Apache Maven Dependency Plugin][42]                    | [Apache License, Version 2.0][31]              |
| [Lombok Maven Plugin][43]                               | [The MIT License][3]                           |
| [Project keeper maven plugin][44]                       | [The MIT License][45]                          |
| [Maven Failsafe Plugin][46]                             | [Apache License, Version 2.0][31]              |
| [JaCoCo :: Maven Plugin][47]                            | [Eclipse Public License 2.0][23]               |
| [error-code-crawler-maven-plugin][48]                   | [MIT License][49]                              |
| [Reproducible Build Maven Plugin][50]                   | [Apache 2.0][35]                               |
| [Maven Clean Plugin][51]                                | [The Apache Software License, Version 2.0][35] |
| [Maven Resources Plugin][52]                            | [The Apache Software License, Version 2.0][35] |
| [Maven Install Plugin][53]                              | [The Apache Software License, Version 2.0][35] |
| [Maven Deploy Plugin][54]                               | [The Apache Software License, Version 2.0][35] |
| [Maven Site Plugin 3][55]                               | [The Apache Software License, Version 2.0][35] |

[0]: https://github.com/exasol/error-reporting-java/
[1]: https://github.com/exasol/error-reporting-java/blob/main/LICENSE
[2]: https://github.com/exasol/virtual-schema-common-document/
[3]: https://opensource.org/licenses/MIT
[4]: https://aws.amazon.com/sdkforjava
[5]: https://aws.amazon.com/apache2.0
[6]: http://www.slf4j.org
[7]: http://www.opensource.org/licenses/mit-license.php
[8]: https://github.com/exasol/hamcrest-resultset-matcher/
[9]: https://github.com/exasol/hamcrest-resultset-matcher/blob/main/LICENSE
[10]: https://projectlombok.org
[11]: https://projectlombok.org/LICENSE
[12]: http://hamcrest.org/JavaHamcrest/
[13]: http://opensource.org/licenses/BSD-3-Clause
[14]: https://junit.org/junit5/
[15]: https://www.eclipse.org/legal/epl-v20.html
[16]: https://github.com/mockito/mockito
[17]: https://github.com/mockito/mockito/blob/main/LICENSE
[18]: https://github.com/exasol/exasol-testcontainers/
[19]: https://github.com/exasol/exasol-testcontainers/blob/main/LICENSE
[20]: https://testcontainers.org
[21]: http://opensource.org/licenses/MIT
[22]: https://www.eclemma.org/jacoco/index.html
[23]: https://www.eclipse.org/legal/epl-2.0/
[24]: https://github.com/exasol/test-db-builder-java/
[25]: https://github.com/exasol/test-db-builder-java/blob/main/LICENSE
[26]: https://github.com/exasol/udf-debugging-java/
[27]: https://github.com/exasol/udf-debugging-java/blob/main/LICENSE
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
[38]: https://maven.apache.org/plugins/maven-assembly-plugin/
[39]: https://maven.apache.org/plugins/maven-jar-plugin/
[40]: https://github.com/exasol/artifact-reference-checker-maven-plugin/
[41]: https://github.com/exasol/artifact-reference-checker-maven-plugin/blob/main/LICENSE
[42]: https://maven.apache.org/plugins/maven-dependency-plugin/
[43]: https://anthonywhitford.com/lombok.maven/lombok-maven-plugin/
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
