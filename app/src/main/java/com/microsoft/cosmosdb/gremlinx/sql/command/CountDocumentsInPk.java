package com.microsoft.cosmosdb.gremlinx.sql.command;

import com.microsoft.cosmosdb.gremlinx.sql.QueryResult;

/**
 * A Command subclass to simply count the documents in a Cosmos DB SQL container
 * with a given partition key value.
 * Chris Joakim, Microsoft
 */

public class CountDocumentsInPk extends Command {

    // Instance variables
    private String pk;

    private CountDocumentsInPk() {
        super();
    }

    public CountDocumentsInPk(String pk) {
        super();
        this.pk = pk;
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

        String sql = queryBuilder.countDocumentsInPkQuery(pk);
        QueryResult result = this.cosmosSqlUtil.executeCountQuery(sql);
        addIntermediateResult(result);

        // Set the actual output for the client
        if (result.getItems().size() > 0) {
            setOutput(result.getItems().get(0));
        }
        else {
            setEmptyJsonOutput();
        }
    }
}
