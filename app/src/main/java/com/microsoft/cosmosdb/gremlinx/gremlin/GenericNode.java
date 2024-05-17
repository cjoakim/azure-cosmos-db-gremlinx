package com.microsoft.cosmosdb.gremlinx.gremlin;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.cosmosdb.gremlinx.AppConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Instances of this class represent either a Gremlin Vertex or Edge data structure.
 * Chris Joakim, Microsoft
 */

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties({ "updated", "edgeKeyCount" })
public class GenericNode implements AppConstants {

    // Constants
    String[] PROPERTIES_TO_PRUNE = {};

    // Class variables
    private static Logger logger = LogManager.getLogger(GenericNode.class);

    // Instance variables:
    public String id;
    public String pk;

    public String cacheKey;
    public String nodeType;
    public String label;

    public String inV;

    public String inVLabel;

    public String outV;
    public String outVLabel;

    public HashMap<String, Object> properties = null;
    public HashMap<String, Object> attributes = null;

    public ArrayList<String> hashtags = null;
    public GenericNode inVNode  = null;
    public GenericNode outVNode = null;
    public boolean updated;

    public HashMap<String, ArrayList<String>> edges;

    public GenericNode() {

        super();
        properties = new HashMap<String, Object>();
        attributes = new HashMap<String, Object>();
        hashtags   = new ArrayList<String>();
        edges      = new HashMap<String, ArrayList<String>>();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPk() {
        return pk;
    }

    public void setPk(String pk) {
        this.pk = pk;
    }

    public String getCacheKey() {
        return cacheKey;
    }

    public void setCacheKey(String cacheKey) {
        this.cacheKey = cacheKey;
    }

    @JsonIgnore
    public String calculateCacheKey() {

        StringBuffer sb = new StringBuffer();
        sb.append("");
        sb.append(this.pk);
        sb.append("|");
        sb.append(this.id);
        cacheKey = sb.toString();
        return cacheKey;
    }

    public String getNodeType() {
        return nodeType;
    }

    public void setNodeType(String nodeType) {
        this.nodeType = nodeType;
    }

    @JsonIgnore
    public boolean isEdge() {
        return getNodeType().equalsIgnoreCase(NODE_TYPE_EDGE);
    }

    @JsonIgnore
    public boolean isVertex() {
        return getNodeType().equalsIgnoreCase(NODE_TYPE_VERTEX);
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getInV() {
        return inV;
    }

    @JsonIgnore
    public String getInVCacheKey() {
        return pk + "|" + inV;
    }

    public void setInV(String inV) {
        this.inV = inV;
    }

    public String getInVLabel() {
        return inVLabel;
    }

    public void setInVLabel(String inVLabel) {
        this.inVLabel = inVLabel;
    }

    public String getOutVLabel() {
        return outVLabel;
    }

    public void setOutVLabel(String outVLabel) {
        this.outVLabel = outVLabel;
    }

    public String getOutV() {
        return outV;
    }
    @JsonIgnore
    public String getOutVCacheKey() {
        return pk + "|" + outV;
    }
    public void setOutV(String outV) {
        this.outV = outV;
    }

    public HashMap<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(HashMap<String, Object> properties) {
        this.properties = properties;
    }

    public void setProperty(String key, Object obj) {

        try {
            if (key != null) {
                if (obj != null) {
                    String className = obj.getClass().getName();
                    if (className.contains("List")) {  // java.util.ArrayList
                        logger.warn("setProperty List value : " + key + " : " + className + " : " + obj);
                        List list = (List) obj;
                        if (list.size() > 0) {
                            Map map = (Map) list.get(0);
                            logger.warn("setProperty Map element : " + map);
                            if (map.containsKey(KEY_VALUE)) {
                                Object value = map.get(KEY_VALUE);
                                properties.put(key, value);
                                logger.warn("setProperty value : " + key + " :" + obj);
                            }
                            else {
                                logger.error("setProperty Map element has no value attribute: " + map);
                            }
                        }
                    }
                    else {
                        logger.error("setProperty Other value : " + key + " : " + className + " : " + obj);
                        properties.put(key, obj);
                    }
                }
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public GenericNode getInVNode() {
        return inVNode;
    }

    public void setInVNode(GenericNode inVNode) {
        this.inVNode = inVNode;
    }

    public GenericNode getOutVNode() {
        return outVNode;
    }

    public void setOutVNode(GenericNode outVNode) {
        this.outVNode = outVNode;
    }

    /**
     * Prune this instance prior to serializing to JSON and loading to CosmosDB/SQL.
     * For now, just the cacheKey used in the migration/transformation process.
     */
    public void pruneBeforeLoad() {
        this.setCacheKey(null);
        this.attributes = null;

        if (isVertex()) {
            if (this.getProperties() != null) {
                for (int i = 0; i < PROPERTIES_TO_PRUNE.length; i++) {
                    String propName = PROPERTIES_TO_PRUNE[i];
                    if (this.getProperties().containsKey(propName)) {
                        this.getProperties().remove(propName);
                    }
                }
            }
            pruneLinkedNode(inVNode);
            pruneLinkedNode(outVNode);
        }
    }

    /**
     * Prune the unnecessary and potentially large values from a linked node in a vertex node.
     */
    public void pruneLinkedNode(GenericNode linkedNode) {
        if (linkedNode != null) {
            linkedNode.attributes = null;
            linkedNode.cacheKey   = null;
            if (linkedNode.getProperties() != null) {
                for (int i = 0; i < PROPERTIES_TO_PRUNE.length; i++) {
                    String propName = PROPERTIES_TO_PRUNE[i];
                    if (linkedNode.getProperties().containsKey(propName)) {
                        linkedNode.getProperties().remove(propName);
                    }
                }
            }
        }
    }

    public void transform() {

        ObjectMapper mapper = new ObjectMapper();

        if (this.properties != null) {
            if (this.properties.containsKey("hashtags")) {
                if (this.isVertex()) {
                    String json = (String) this.properties.get("hashtags");
                    try {
                        String[] array = mapper.readValue(json, String[].class);
                        //logger.error("transform - parsed_tags: " + json + " -> " + array.length);
                        for (int i = 0; i < array.length; i++) {
                            this.hashtags.add(array[i]);
                        }

                    }
                    catch (JsonProcessingException e) {
                        logger.error("transform - unable to parse: " + json);
                        e.printStackTrace();
                    }
                }
                this.properties.remove("hashtags");
            }
        }
    }

    public int getEdgeKeyCount() {

        return edges.size();
    }

    public boolean addEdge(String tag, String id) {

        if (tag == null) {
            logger.warn("addEdge, given tag is null");
            return false;
        }
        if (id == null) {
            logger.warn("addEdge, given id is null");
            return false;
        }

        if (this.edges.containsKey(tag)) {
            ArrayList<String> list = edges.get(tag);
            if (list.contains(id)) {
                logger.warn("addEdge, omitting duplicate: " + tag + " -> " + id);
            }
            else {
                list.add(id.trim());
                logger.warn("addEdge, added: " + tag + " -> " + id);
                this.updated = true;
            }
        }
        else {
            ArrayList<String> list = new ArrayList<String>();
            list.add(id.trim());
            edges.put(tag, list);
            logger.warn("addEdge, added new tag: " + tag + " -> " + id);
            this.updated = true;
        }
        return this.updated;
    }

    public String toJson() throws JsonProcessingException {

        return (new ObjectMapper()).writerWithDefaultPrettyPrinter().writeValueAsString(this);
    }
}
