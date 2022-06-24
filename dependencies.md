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
| [Project Lombok][10]                             | [The MIT License][11]            |

## Test Dependencies

| Dependency                                      | License                           |
| ----------------------------------------------- | --------------------------------- |
| [Hamcrest][12]                                  | [BSD License 3][13]               |
| [JUnit Jupiter Engine][14]                      | [Eclipse Public License v2.0][15] |
| [JUnit Jupiter Params][14]                      | [Eclipse Public License v2.0][15] |
| [mockito-junit-jupiter][18]                     | [The MIT License][19]             |
| [Common Virtual Schema for document data][2]    | [MIT][1]                          |
| [Test containers for Exasol on Docker][22]      | [MIT][1]                          |
| [Testcontainers :: JUnit Jupiter Extension][24] | [MIT][25]                         |
| [JaCoCo :: Agent][26]                           | [Eclipse Public License 2.0][27]  |
| [JaCoCo :: Core][26]                            | [Eclipse Public License 2.0][27]  |
| [Test Database Builder for Java][30]            | [MIT License][31]                 |
| [udf-debugging-java][32]                        | [MIT][1]                          |

## Plugin Dependencies

| Dependency                                              | License                                        |
| ------------------------------------------------------- | ---------------------------------------------- |
| [SonarQube Scanner for Maven][34]                       | [GNU LGPL 3][35]                               |
| [Apache Maven Compiler Plugin][36]                      | [Apache License, Version 2.0][37]              |
| [Apache Maven Enforcer Plugin][38]                      | [Apache License, Version 2.0][37]              |
| [Maven Flatten Plugin][40]                              | [Apache Software Licenese][41]                 |
| [org.sonatype.ossindex.maven:ossindex-maven-plugin][42] | [ASL2][41]                                     |
| [Reproducible Build Maven Plugin][44]                   | [Apache 2.0][41]                               |
| [Maven Surefire Plugin][46]                             | [Apache License, Version 2.0][37]              |
| [Versions Maven Plugin][48]                             | [Apache License, Version 2.0][37]              |
| [Apache Maven Assembly Plugin][50]                      | [Apache License, Version 2.0][37]              |
| [Apache Maven JAR Plugin][52]                           | [Apache License, Version 2.0][37]              |
| [Artifact reference checker and unifier][54]            | [MIT][1]                                       |
| [Apache Maven Dependency Plugin][56]                    | [Apache License, Version 2.0][37]              |
| [Lombok Maven Plugin][58]                               | [The MIT License][1]                           |
| [Project keeper maven plugin][60]                       | [The MIT License][61]                          |
| [Maven Failsafe Plugin][62]                             | [Apache License, Version 2.0][37]              |
| [JaCoCo :: Maven Plugin][64]                            | [Eclipse Public License 2.0][27]               |
| [error-code-crawler-maven-plugin][66]                   | [MIT][1]                                       |
| [Maven Clean Plugin][68]                                | [The Apache Software License, Version 2.0][41] |
| [Maven Resources Plugin][70]                            | [The Apache Software License, Version 2.0][41] |
| [Maven Install Plugin][72]                              | [The Apache Software License, Version 2.0][41] |
| [Maven Deploy Plugin][74]                               | [The Apache Software License, Version 2.0][41] |
| [Maven Site Plugin 3][76]                               | [The Apache Software License, Version 2.0][41] |

[26]: https://www.eclemma.org/jacoco/index.html
[0]: https://github.com/exasol/error-reporting-java
[41]: http://www.apache.org/licenses/LICENSE-2.0.txt
[10]: https://projectlombok.org
[46]: https://maven.apache.org/surefire/maven-surefire-plugin/
[68]: http://maven.apache.org/plugins/maven-clean-plugin/
[4]: https://aws.amazon.com/sdkforjava
[1]: https://opensource.org/licenses/MIT
[18]: https://github.com/mockito/mockito
[40]: https://www.mojohaus.org/flatten-maven-plugin/
[48]: http://www.mojohaus.org/versions-maven-plugin/
[60]: https://github.com/exasol/project-keeper/
[13]: http://opensource.org/licenses/BSD-3-Clause
[36]: https://maven.apache.org/plugins/maven-compiler-plugin/
[31]: https://github.com/exasol/test-db-builder-java/blob/main/LICENSE
[27]: https://www.eclipse.org/legal/epl-2.0/
[35]: http://www.gnu.org/licenses/lgpl.txt
[64]: https://www.jacoco.org/jacoco/trunk/doc/maven.html
[5]: https://aws.amazon.com/apache2.0
[19]: https://github.com/mockito/mockito/blob/main/LICENSE
[8]: https://github.com/exasol/hamcrest-resultset-matcher
[11]: https://projectlombok.org/LICENSE
[44]: http://zlika.github.io/reproducible-build-maven-plugin
[7]: http://www.opensource.org/licenses/mit-license.php
[34]: http://sonarsource.github.io/sonar-scanner-maven/
[2]: https://github.com/exasol/virtual-schema-common-document/
[32]: https://github.com/exasol/udf-debugging-java/
[14]: https://junit.org/junit5/
[12]: http://hamcrest.org/JavaHamcrest/
[6]: http://www.slf4j.org
[70]: http://maven.apache.org/plugins/maven-resources-plugin/
[54]: https://github.com/exasol/artifact-reference-checker-maven-plugin
[52]: https://maven.apache.org/plugins/maven-jar-plugin/
[30]: https://github.com/exasol/test-db-builder-java/
[62]: https://maven.apache.org/surefire/maven-failsafe-plugin/
[25]: http://opensource.org/licenses/MIT
[22]: https://github.com/exasol/exasol-testcontainers
[56]: https://maven.apache.org/plugins/maven-dependency-plugin/
[61]: https://github.com/exasol/project-keeper/blob/main/LICENSE
[37]: https://www.apache.org/licenses/LICENSE-2.0.txt
[38]: https://maven.apache.org/enforcer/maven-enforcer-plugin/
[15]: https://www.eclipse.org/legal/epl-v20.html
[72]: http://maven.apache.org/plugins/maven-install-plugin/
[42]: https://sonatype.github.io/ossindex-maven/maven-plugin/
[24]: https://testcontainers.org
[58]: https://anthonywhitford.com/lombok.maven/lombok-maven-plugin/
[74]: http://maven.apache.org/plugins/maven-deploy-plugin/
[76]: http://maven.apache.org/plugins/maven-site-plugin/
[66]: https://github.com/exasol/error-code-crawler-maven-plugin
[50]: https://maven.apache.org/plugins/maven-assembly-plugin/
