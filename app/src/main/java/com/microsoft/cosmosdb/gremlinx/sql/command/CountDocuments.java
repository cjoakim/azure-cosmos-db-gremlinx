package com.microsoft.cosmosdb.gremlinx.sql.command;

import com.microsoft.cosmosdb.gremlinx.sql.QueryResult;

/**
 * A Command subclass to simply count the documents in a Cosmos DB SQL container
 * with a given partition key value.
 * Chris Joakim, Microsoft
 */

public class CountDocuments extends Command {

    // Instance variables
    private String pk;

    public CountDocuments() {
        super();
    }

    @Override
    protected boolean validateParameters() throws Exception {

        return true;
    }

    @Override
    protected void executeActions() throws Exception {

        String sql = queryBuilder.countDocumentsQuery();
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
