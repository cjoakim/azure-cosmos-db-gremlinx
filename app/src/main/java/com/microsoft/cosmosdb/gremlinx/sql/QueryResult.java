package com.microsoft.cosmosdb.gremlinx.sql;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.microsoft.cosmosdb.gremlinx.AppConstants;

/**
 * Instances of this class represent the result of a CosmosDB SQL API query
 * which returns documents.
 * Chris Joakim, Microsoft
 */
@JsonInclude(JsonInclude.Include.NON_NULL)  // <-- don't serialize the null attributes to JSON
public class QueryResult extends CosmosResult implements AppConstants {

    // Instance variables:
    protected String sql;

    public QueryResult(String sql) {

        super();
        this.sql = sql;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
}
