package com.microsoft.cosmosdb.gremlinx.sql;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.microsoft.cosmosdb.gremlinx.AppConstants;

/**
 * Instances of this class represent the result of a CosmosDB SQL API CRUD operation.
 * Chris Joakim, Microsoft
 */

@JsonInclude(JsonInclude.Include.NON_NULL)  // <-- don't serialize the null attributes to JSON
public class CrudResult extends CosmosResult implements AppConstants {

    public CrudResult() {

        super();
    }

}
