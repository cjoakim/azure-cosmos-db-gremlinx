# azure-cosmos-db-gremlinx

Exports data from a Cosmos DB Gremlin account via the NoSQL endpoint and Java

---

## System Requirements

- Java JDK 17 or higher
- Gradle 8.7 or higher
- Windows 11, Linux, or macOS OS
- Network connectivity to Maven Central to download the required JAR files
- Network connectivity to the source Cosmos DB Gramlin account

---

## Environment Variables

This utility uses the following environment variables, you should
set these per your Cosmos DB and system configuration.

```
GREMLIN_NOSQL_URL=<cosmos-db-sql-endpoint-url>
GREMLIN_NOSQL_KEY=<cosmos-db-sql-endpoint-key>
GREMLIN_NOSQL_DB=<db-name>
GREMLIN_NOSQL_COLL=<graph-container-name>
GREMLIN_NOSQL_EXPORTS_DIR=<output-directory-for-exported-json-files>
```

A **.env** file, in the app/ directory, can be used to define these environment variables.
This file is Git-Ignored in this repo.  See file app/sample_dot_env.

---

## Executing the Export Process

### Compile the Java code

```
> gradle build

gradle build
Starting a Gradle Daemon, 1 busy Daemon could not be reused, use --status for details

BUILD SUCCESSFUL in 3s
```

### Verify your Environment Variables

```
> gradle checkEnv

... output omitted ...
```

### Execute the Export

- Ensure that directory app/exports exists and is empty
- Ensure that directory app/tmp exists and is empty

```
> gradle exportGremlinViaSqlEndpoint

... output omitted ...
```

Then see the contents of the app/exports directory.
