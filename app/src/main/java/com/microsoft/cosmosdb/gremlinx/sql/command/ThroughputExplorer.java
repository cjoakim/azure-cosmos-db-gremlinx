package com.microsoft.cosmosdb.gremlinx.sql.command;

import com.azure.cosmos.models.ThroughputResponse;

/**
 * A Command subclass explore the database-level and/or container-level throughput
 * settings and state for a Cosmos DB account.
 * Chris Joakim, Microsoft
 */

public class ThroughputExplorer extends Command {

    // Instance variables
    private String dbName;
    private String cName;

    private ThroughputExplorer() {
        super();
    }

    public ThroughputExplorer(String dbName, String cName) {
        super();
        this.dbName = dbName;
        this.cName = cName;
    }

    @Override
    protected boolean validateParameters() throws Exception {

        if (dbName == null) {
            return false;
        }
        // cName is optional
        return true;
    }

    @Override
    protected void executeActions() throws Exception {

        ThroughputResponse throughputResp = null;
        try {
            this.cosmosSqlUtil.setCurrentDatabase(dbName);
            throughputResp = this.cosmosSqlUtil.getCurrentDatabase().readThroughput().block();
            logger.warn("Throughput - dbName: " + dbName);
            logger.warn("minThroughput:            " + throughputResp.getMinThroughput());
            logger.warn("manual throughput:        " + throughputResp.getProperties().getManualThroughput());
            logger.warn("max autoscale throughput: " + throughputResp.getProperties().getAutoscaleMaxThroughput());
        }
        catch (Exception e) {
            logger.warn("no database level throughput for dbName: " + dbName);
        }

        Thread.sleep(500);

        if (cName != null) {
            try {
                getCosmosSqlUtil().setCurrentContainer(cName);
                throughputResp = this.cosmosSqlUtil.getCurrentContainer().readThroughput().block();
                logger.warn("Throughput - dbName: " + dbName + ", cName: " + cName);
                logger.warn("minThroughput:            " + throughputResp.getMinThroughput());
                logger.warn("manual throughput:        " + throughputResp.getProperties().getManualThroughput());
                logger.warn("max autoscale throughput: " + throughputResp.getProperties().getAutoscaleMaxThroughput());
            }
            catch (Exception e) {
                logger.warn("no container level throughput for cName: " + cName + " in dbName: " + dbName);
            }
        }
    }
}
