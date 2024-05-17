package com.microsoft.cosmosdb.gremlinx.sql.command;

import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.util.CosmosPagedFlux;
import reactor.core.publisher.Flux;

import java.util.ArrayList;

/**
 * A Command subclass to list the containers in a Cosmos DB database.
 * Chris Joakim, Microsoft
 */

public class ListDatabases extends Command {

    // Instance variables
    private String dbName;

    public ListDatabases() {
        super();
    }


    @Override
    protected boolean validateParameters() throws Exception {

        return true;
    }

    @Override
    protected void executeActions() throws Exception {

        logger.warn("executeActions, dbName: " + dbName);

        ArrayList<String> containerNames = new ArrayList<>();
        getCosmosSqlUtil().setCurrentDatabase(dbName);
        CosmosPagedFlux<CosmosContainerProperties> containers =
                this.cosmosSqlUtil.getCurrentDatabase().readAllContainers();

        containers.byPage(100).flatMap(readAllContainersResponse -> {
            logger.info("read {} containers(s) with request charge of {}",
                    readAllContainersResponse.getResults().size(),readAllContainersResponse.getRequestCharge());

            for (CosmosContainerProperties response : readAllContainersResponse.getResults()) {
                String cname = response.getId();
                logger.info("container id: {}", cname);
                containerNames.add(cname);
            }
            return Flux.empty();
        }).blockLast();

        String outfile = "tmp/list_containers_" + dbName + ".json";
        writeJson(containerNames, outfile, true, true);
    }
}
