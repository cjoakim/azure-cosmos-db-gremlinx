package com.microsoft.cosmosdb.gremlinx.sql.command;

import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.util.CosmosPagedFlux;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.microsoft.cosmosdb.gremlinx.AppConfig;
import com.microsoft.cosmosdb.gremlinx.io.FileUtil;
import com.microsoft.cosmosdb.gremlinx.sql.QueryResult;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * A Command subclass to export all the documents from a given partition key,
 * container, and database.
 * Chris Joakim, Microsoft
 */

public class ExportDocuments extends Command {

    // Instance variables
    private ObjectWriter objectWriter;
    private FileUtil fileUtil;

    private int batchSize = 1000;
    private int batchNumber = 0;

    public ExportDocuments() {
        super();
        this.objectWriter = (new ObjectMapper()).writerWithDefaultPrettyPrinter();
        this.fileUtil = new FileUtil();
    }

    @Override
    protected boolean validateParameters() throws Exception {
        return true;
    }

    @Override
    protected void executeActions() throws Exception {
        String exportsDir = AppConfig.getEnvVar("GREMLIN_NOSQL_EXPORTS_DIR");
        logger.warn("exports directory is: " + exportsDir);

        String sql = queryBuilder.allDocumentsQuery();
        logger.warn(sql);
        QueryResult qr = new QueryResult(sql);
        CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();
        CosmosPagedFlux<Map> flux = getCosmosSqlUtil().getCurrentContainer().queryItems(sql, queryOptions, Map.class);

        final ArrayList<Map> documentBatch = new ArrayList<>();
        AtomicLong counter = new AtomicLong();

        flux.byPage(DEFAULT_SQL_QUERY_PAGE_SIZE).flatMap(fluxResponse -> {
            List<Map> results = fluxResponse.getResults().stream().collect(Collectors.toList());
            qr.incrementTotalRequestUnits(fluxResponse.getRequestCharge());
            for (int r = 0; r < results.size(); r++) {
                counter.incrementAndGet();
                documentBatch.add(results.get(r));
                if (documentBatch.size() == batchSize) {
                    batchNumber++;
                    String outfile = exportsDir + "/export_nosql_batch_" + batchNumber + ".json";
                    try {
                        fileUtil.writeJson(documentBatch, outfile, true, true);
                    }
                    catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    documentBatch.clear();
                }
            }
            return Flux.empty();
        }).blockLast();

        if (documentBatch.size() > 0) {
            logger.warn("writing last batch...");
            batchNumber++;
            String outfile = exportsDir + "/export_nosql_batch" + batchNumber + "_e" + System.currentTimeMillis() + ".json";
            fileUtil.writeJson(documentBatch, outfile, true, true);
        }
        logger.warn("document count: " + counter.get());
    }

    private String toJson(Object obj) {
        try {
            return objectWriter.writeValueAsString(obj);
        }
        catch (JsonProcessingException e) {
            logger.error("unable to parse object to json: " + obj);
            return null;
        }
    }
}
