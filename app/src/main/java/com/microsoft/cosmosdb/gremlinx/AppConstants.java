package com.microsoft.cosmosdb.gremlinx;
/**
 * This interface defines constant values used in this application.
 * Chris Joakim, Microsoft
 */

public interface AppConstants {

    // Response Codes
    public int RESPONSE_CODE_SUCCESSFUL     = 200;
    public int RESPONSE_CODE_INVALID_PARAMS = 400;
    public int RESPONSE_CODE_SERVER_ERROR   = 500;

    public int DEFAULT_SQL_QUERY_PAGE_SIZE  = 1000;

    public String KEY_VALUE                 = "value";

    public String NODE_TYPE_VERTEX          = "vertex";
    public String NODE_TYPE_EDGE            = "edge";
}
