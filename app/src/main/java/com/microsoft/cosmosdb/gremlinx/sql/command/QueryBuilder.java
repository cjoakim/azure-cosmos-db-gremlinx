package com.microsoft.cosmosdb.gremlinx.sql.command;

import com.microsoft.cosmosdb.gremlinx.AppConstants;

/**
 * Instances of this class build SQL String values to be executed by CosmosSqlUtil.
 * Chris Joakim, Microsoft
 */

public class QueryBuilder implements AppConstants {

    public QueryBuilder() {

        super();
    }

    public String countDocumentsQuery() {

        return "select count(1) as count from c";  // alternatively: "select count(1) from c";
    }

    public String countDocumentsInPkQuery(String pk) {

        return "select count(1) as count from c where c.pk = '" + pk + "'";
    }

    public String allDocumentsQuery() {

        return "select * from c";
    }

    public String allDocumentsInPkQuery(String pk) {

        return "select * from c where c.pk = '" + pk + "'";
    }

    public String allVerticesInPkQuery(String pk) {

        String nodeType = "vertex";
        return "select * from c where c.pk = '" + pk + "' and c.nodeType = '" + nodeType + "'";
    }

    public String allEdgesInPkQuery(String pk) {

        String nodeType = "edge";
        return "select * from c where c.pk = '" + pk + "' and c.nodeType = '" + nodeType + "'";
    }

    public String pointReadQuery(String pk, String id) {

        return "select * from c where c.pk = '" + pk + "' and c.id = '" + id + "' offset 0 limit 1";
    }
}
