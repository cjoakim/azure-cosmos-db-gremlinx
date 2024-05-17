package com.microsoft.cosmosdb.gremlinx.sql;


import com.azure.cosmos.*;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.util.CosmosPagedFlux;
import com.microsoft.cosmosdb.gremlinx.AppConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility class for CosmosDB/SQL API operations.
 * Chris Joakim, Microsoft
 */

public class CosmosSqlUtil implements AppConstants {

    // Class variables
    private static Logger logger = LogManager.getLogger(CosmosSqlUtil.class);

    // Instance variables
    protected String uri;
    protected String key;
    protected CosmosAsyncClient client;
    protected CosmosAsyncDatabase currentDatabase;
    protected CosmosAsyncContainer currentContainer;

    public CosmosSqlUtil(String url, String key) {

        super();

        try {
            client = new CosmosClientBuilder()
                    .endpoint(url)
                    .key(key)
                    .consistencyLevel(ConsistencyLevel.EVENTUAL)
                    .contentResponseOnWriteEnabled(true)
                    .buildAsyncClient();
            logger.warn("client created");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public CosmosAsyncClient getClient() {

        return client;
    }

    public CosmosAsyncDatabase getCurrentDatabase() {

        return currentDatabase;
    }

    public String getCurrentDatabaseName() {

        if (currentDatabase == null) {
            return null;
        }
        else {
            return currentDatabase.getId();
        }
    }

    public void setCurrentDatabase(String dbName) {

        currentDatabase = client.getDatabase(dbName);
    }

    public CosmosAsyncContainer getCurrentContainer() {

        return currentContainer;
    }

    public String getCurrentContainerName() {

        if (currentContainer == null) {
            return null;
        }
        else {
            return currentContainer.getId();
        }
    }

    public void setCurrentContainer(String containerName) {

        currentContainer  = currentDatabase.getContainer(containerName);
    }

    public QueryResult executeCountQuery(String sql) {

        logger.warn("executeCountQuery, sql: " + sql);
        QueryResult qr = new QueryResult(sql);

        CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();
        CosmosPagedFlux<Map> flux = getCurrentContainer().queryItems(sql, queryOptions, Map.class);

        flux.byPage(DEFAULT_SQL_QUERY_PAGE_SIZE).flatMap(fluxResponse -> {
            List<Map> results = fluxResponse.getResults().stream().collect(Collectors.toList());
            qr.incrementTotalRequestUnits(fluxResponse.getRequestCharge());
            for (int r = 0; r < results.size(); r++) {
                qr.addItem(results.get(r));
            }
            return Flux.empty();
        }).blockLast();

        return qr;
    }

    public void close() {

        if (client != null) {
            logger.warn("closing client...");
            client.close();
            logger.warn("client closed");
        }
    }
}

