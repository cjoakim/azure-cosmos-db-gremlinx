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

public class ExportDocumentsInPk extends Command {

    // Instance variables
    private String pk;
    private ObjectWriter objectWriter;
    private FileUtil fileUtil;

    private int batchSize = 1000;
    private int batchNumber = 0;

    private ExportDocumentsInPk() {
        super();
    }

    public ExportDocumentsInPk(String pk) {
        super();
        this.pk = pk;
        this.objectWriter = (new ObjectMapper()).writerWithDefaultPrettyPrinter();
        this.fileUtil = new FileUtil();
    }

    @Override
    protected boolean validateParameters() throws Exception {
        if (pk == null) {
            return false;
        }
        return true;
    }

    @Override
    protected void executeActions() throws Exception {
        String outdir = AppConfig.getEnvVar("");
        logger.warn("output directory is: " + outdir);

        String sql = queryBuilder.allDocumentsInPkQuery(pk);
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
                    String outfile = outdir + "/export_nosql_pk_" + this.pk + "_batch" + batchNumber + "_e" + System.currentTimeMillis() + ".json";
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
            String outfile = outdir + "/export_nosql_pk_" + this.pk + "_batch" + batchNumber + "_e" + System.currentTimeMillis() + ".json";
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
