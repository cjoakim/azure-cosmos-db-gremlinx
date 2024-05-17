package com.microsoft.cosmosdb.gremlinx.sql;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.microsoft.cosmosdb.gremlinx.AppConstants;

import java.util.Map;

/**
 * Instances of this class represent the result of a CosmosDB SQL API count query.
 * Chris Joakim, Microsoft
 */

@JsonInclude(JsonInclude.Include.NON_NULL)  // <-- don't serialize the null attributes to JSON
public class CountResult extends CosmosResult implements AppConstants {

    // Instance variables:
    protected long count;

    public CountResult() {

        super();
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @JsonIgnore
    public long getCountResult() {
        try {
            Map firstItem = (Map) items.get(0);
            return (long) firstItem.get("count");
        }
        catch (Exception e) {
            return -1;
        }
    }
}