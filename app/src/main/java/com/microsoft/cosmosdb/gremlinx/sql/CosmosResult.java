package com.microsoft.cosmosdb.gremlinx.sql;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;

/**
 * This is the abstract superclass of classes CrudResult, QueryResult, and CountResult.
 * Chris Joakim, Microsoft
 */

@JsonInclude(JsonInclude.Include.NON_NULL)  // <-- don't serialize the null attributes to JSON
public abstract class CosmosResult {

    // Instance variables:
    protected ArrayList<Object> items;
    protected double totalRequestUnits;

    public CosmosResult() {

        super();
        this.items = new ArrayList<Object>();
        this.totalRequestUnits = 0.0;
    }

    public ArrayList<Object> getItems() {
        return items;
    }

    public void setItems(ArrayList<Object> items) {
        this.items = items;
    }

    public void addItem(Object item) {
        items.add(item);
    }

    public double getTotalRequestUnits() {
        return totalRequestUnits;
    }

    @JsonIgnore
    public void incrementTotalRequestUnits(double d) {
        this.totalRequestUnits = this.totalRequestUnits + d;
    }
}
