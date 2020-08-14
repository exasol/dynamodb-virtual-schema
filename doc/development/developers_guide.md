# Developers Guide

This page contains information for developers.

## Integration tests

For the integration tests you can select between three different test environments:

|Setup                          |Description                                |
|-------------------------------|-------------------------------------------|
| `testcontainers` (default)    | For each integration test class a separate docker container is started. This method provides best isolation. It is used by the CI tests.|
|`local`                        | This method is faster than `testcontainers` since it reuses the same container all the time. You must start the container manually by running `docker-compose up` in `src/test/resources/`| 
|`aws`                          | This method fits best for performance tests. You have to create the cluster setup first. You can find more information in the [Performance Testing](#performance-testing) section 

You can select the test environment by adding `-Dtests.testSetup="<selected setup>"` as JVM option for the integration tests. 

## Logging & Debugging

The following links explain logging and debugging for the Virtual Schema in general:

* [Logging for Virtual Schemas](https://github.com/exasol/virtual-schemas/blob/master/doc/development/remote_logging.md)
* [Remote debugging Virtual Schemas](https://github.com/exasol/virtual-schemas/blob/master/doc/development/remote_debugging.md)

For logging and debugging in the test code, things are easier, 
as most of the setup code is already included in the AbstractExasolTestInterface.

The tests automatically redirect the logs from the Virtual Schema to the tests command line output.

For debugging, start a debugger on your development machine listening on port `8000` and 
start the tests with: `-Dtests.debug="virtualSchema"` or `-Dtests.debug="all"`. 
The last option also starts the debugger for the UDF calls.
Since this does not work with multiple UDFs in parallel, the test setup sets `MAX_PARALLEL_UDFS` automatically to 1. 
 
## Profiling

The test setup can run the Virtual Schema and the UDFs with a profiler. 
We use the [Hones Profiler](https://github.com/jvm-profiling-tools/honest-profiler).
To use it, download the binaries from the project homepage 
and place the `liblagent.so` in the directory above this projects root.
Then enable profiling by adding `-Dtests.profiling="true"` to your jvm parameters.

The profiling results will be stored in the `/tmp` directory of the UDF containers.
Since this directories are deleted directly after the UDF execution, accessing the files is a bit complicated.
To grab them run the `tools/profilingFileSaver.py` script on the host of the Exasol database (typically an AWS data node or a docker-container).
The script monitors the UDF directories and hard-links the profiling results to `/tmp`. By that they are not automatically deleted and you can transfer them to your development PC, for example using `scp`.
Then you can analyze them using the Hones Profiler GUI.

## Performance Testing

This repository includes performance tests.
They are designed to run on AWS.
That means that Exasol and DynamoDB run both on AWS.
You can create this setup, using a [terraform](https://www.terraform.io/) script.
The only thing you need to do is run `terraform apply` in the `cloudSetup` folder.

Next you need to fill the DynamoDB with data.
For the performance tests we used the [OpenLibrary data set](https://openlibrary.org/data).
You can load it into the a DynamoDB table by running the `OpenLibrary` class on a AWS EC2 instance.
For that purpose we included the `test_runner` instance in the terraform script.
Refer to the class comment of the `OpenLibrary` class for the more detailed steps.

After you imported the data you can run the performance tests.
The tests are bundled in the `DynamodbAdapterPerformanceIT` class. You can run it on your local dev machine by setting `-Dtests.testSetup="aws"` property.

For comparison we also included tests that use the [CData JDBC connector](https://www.cdata.com/drivers/dynamodb/jdbc/). 
You can find these tests in the `TestCdataConnectorIT` class.
