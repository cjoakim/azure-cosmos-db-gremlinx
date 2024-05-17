package com.microsoft.cosmosdb.gremlinx;

import com.azure.cosmos.models.CosmosDatabaseProperties;
import com.azure.cosmos.util.CosmosPagedFlux;
import com.microsoft.cosmosdb.gremlinx.io.FileUtil;
import com.microsoft.cosmosdb.gremlinx.sql.CosmosSqlUtil;
import com.microsoft.cosmosdb.gremlinx.sql.command.CountDocuments;
import com.microsoft.cosmosdb.gremlinx.sql.command.ExportDocuments;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Flux;

import java.util.ArrayList;

/**
 * Entry point for this application, contains a main() method.
 * Chris Joakim, Microsoft
 */

public class App implements AppConstants {

    // Class variables
    private static Logger logger = LogManager.getLogger(App.class);

    public static void main(String[] args) {

        if (args.length < 1) {
            logger.error("No command-line args; terminating...");
        }
        else {
            try {
                AppConfig.setCommandLineArgs(args);
                String function = args[0];
                String dbName = null;
                String containerName = null;

                switch (function) {

                    case "checkEnv":
                        logger.warn("GREMLIN_NOSQL_URL:  " + AppConfig.getEnvVar("GREMLIN_NOSQL_URL"));
                        logger.warn("GREMLIN_NOSQL_KEY:  " + AppConfig.getEnvVar("GREMLIN_NOSQL_KEY"));
                        logger.warn("GREMLIN_NOSQL_DB:   " + AppConfig.getEnvVar("GREMLIN_NOSQL_DB"));
                        logger.warn("GREMLIN_NOSQL_COLL: " + AppConfig.getEnvVar("GREMLIN_NOSQL_COLL"));
                        break;
                    case "exportGremlinViaSqlEndpoint":
                        exportGremlinViaSqlEndpoint();
                        break;

                    default:
                        logger.error("unknown main function: " + function);
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.exit(0);
    }

    /**
     * Export documents from the given Gremlin db, container, and pk using the Cosmos DB SQL API.
     */
    private static void exportGremlinViaSqlEndpoint() {

        CosmosSqlUtil gremlinSqlUtil = null;
        FileUtil fu = new FileUtil();
        String url    = AppConfig.getEnvVar("GREMLIN_NOSQL_URL");
        String key    = AppConfig.getEnvVar("GREMLIN_NOSQL_KEY");
        String dbName = AppConfig.getEnvVar("GREMLIN_NOSQL_DB");
        String cName  = AppConfig.getEnvVar("GREMLIN_NOSQL_COLL");
        logger.warn("exportGremlinViaSqlEndpoint, url:   " + url);
        logger.warn("exportGremlinViaSqlEndpoint, key:   " + key);
        logger.warn("exportGremlinViaSqlEndpoint, db:    " + dbName);
        logger.warn("exportGremlinViaSqlEndpoint, cname: " + cName);
        try {
            gremlinSqlUtil = new CosmosSqlUtil(url, key);
            gremlinSqlUtil.setCurrentDatabase(dbName);
            gremlinSqlUtil.setCurrentContainer(cName);

            String outfile = null;

            ArrayList<String> databaseNames = new ArrayList<>();

            CosmosPagedFlux<CosmosDatabaseProperties> databases =
                    gremlinSqlUtil.getClient().readAllDatabases();
            databases.byPage(100).flatMap(readAllDatabasesResponse -> {
                for (CosmosDatabaseProperties response : readAllDatabasesResponse.getResults()) {
                    String dbid = response.getId();
                    logger.info("db id: {}", dbid);
                    logger.info("db resource id: {}", response.getResourceId());
                    databaseNames.add(dbid);
                }
                return Flux.empty();
            }).blockLast();
            outfile = "tmp/list_databases.json";
            fu.writeJson(databaseNames, outfile, true, true);

            // count the documents in the container
            CountDocuments countDocuments = new CountDocuments();
            countDocuments.execute(gremlinSqlUtil);
            outfile = "tmp/export_count_documents.json";
            fu.writeJson(countDocuments, outfile, true, true);

            // Execute the export to a set of local json files if the --export CLI arg is present
            if (AppConfig.booleanArg("--export")) {
                ExportDocuments exporter = new ExportDocuments();
                exporter.execute(gremlinSqlUtil);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            if (gremlinSqlUtil != null) {
                gremlinSqlUtil.close();
            }
        }
    }
}
