package com.microsoft.cosmosdb.gremlinx.sql.command;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.microsoft.cosmosdb.gremlinx.AppConstants;
import com.microsoft.cosmosdb.gremlinx.io.FileUtil;
import com.microsoft.cosmosdb.gremlinx.sql.CosmosResult;
import com.microsoft.cosmosdb.gremlinx.sql.CosmosSqlUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Abstract superclass of the various "command" (or use-case) classes
 * in this package which interact with CosmosDB.
 * Chris Joakim, Microsoft
 */

public abstract class Command implements AppConstants {

    // Class variables:
    protected static Logger logger = LogManager.getLogger(Command.class);

    // Instance variables:
    protected CosmosSqlUtil cosmosSqlUtil = null;  // Contains the connection to CosmosDB and helper methods
    protected QueryBuilder  queryBuilder = null;   // Reusable common class to generate SQL statements

    protected String commandName;               // The name of the class
    protected int    responseCode;              // Either 200, 400, or 500, see the execute() method
    protected String errorMessage;
    protected long   elapsedMs;                 // total elapsed milliseconds, see the execute() method
    protected double totalRequestUnits;         // Total CosmosDB RU costs for all operations

    protected ArrayList<CosmosResult> intermediateResults;  // for development & debugging

    protected Object output;                    // The actual output to be sent to the client


    protected Command() {
        super();
        this.commandName = this.getClass().getSimpleName();
        queryBuilder     = new QueryBuilder();
        intermediateResults = new ArrayList<CosmosResult>();
    }

    /**
     * Subclasses should execute this method but not override it.
     */
    public final void execute(CosmosSqlUtil cosmosSqlUtil) throws Exception {

        long startMs = System.currentTimeMillis();
        this.cosmosSqlUtil = cosmosSqlUtil;

        if (validateParameters()) {
            try {
                executeActions();
                responseCode = RESPONSE_CODE_SUCCESSFUL;
            }
            catch (Exception e) {
                responseCode = RESPONSE_CODE_SERVER_ERROR;
                e.printStackTrace();
            }
        }
        else {
            responseCode = RESPONSE_CODE_INVALID_PARAMS;
        }
        elapsedMs = System.currentTimeMillis() - startMs;
    }

    /**
     * Validate the parameters to this Command; return true if valid, false if not.
     * Subclasses must implement this method.
     */
    protected abstract boolean validateParameters() throws Exception;

    /**
     * Execute the one or more database operations in this Command.
     * Subclasses must implement this method.
     */
    protected abstract void executeActions() throws Exception;

    @JsonIgnore
    public CosmosSqlUtil getCosmosSqlUtil() {
        return cosmosSqlUtil;
    }

    @JsonIgnore
    public QueryBuilder getQueryBuilder() {
        return queryBuilder;
    }

    public String getCommandName() {
        return commandName;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public void setResponseCode(int responseCode) {
        this.responseCode = responseCode;
    }

    public long getElapsedMs() {
        return elapsedMs;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public ArrayList<CosmosResult> getIntermediateResults() {
        return intermediateResults;
    }

    public void setIntermediateResults(ArrayList<CosmosResult> intermediateResults) {
        this.intermediateResults = intermediateResults;
    }

    @JsonIgnore
    public void addIntermediateResult(CosmosResult result) {
        this.intermediateResults.add(result);
        addTotalRequestUnits(result.getTotalRequestUnits());
    }

    public Object getOutput() {
        return output;
    }

    public void setOutput(Object output) {
        this.output = output;
    }

    public void initializeOutputList() {
        this.output = new ArrayList<Object>();
    }

    public void addOutputItem(Object item) {
        ((ArrayList) this.output).add(item);
    }

    public void setEmptyJsonOutput() {
        this.output = new HashMap<String, String>();
    }

    @JsonIgnore
    public void addTotalRequestUnits(double incrementalRequestUnits) {
        this.totalRequestUnits = this.totalRequestUnits + incrementalRequestUnits;
    }

    public double getTotalRequestUnits() {
        return totalRequestUnits;
    }

    public void writeJson(Object obj, String outfile, boolean pretty, boolean verbose) throws Exception {
        FileUtil fu = new FileUtil();
        fu.writeJson(obj, outfile, true, true);
    }

}
