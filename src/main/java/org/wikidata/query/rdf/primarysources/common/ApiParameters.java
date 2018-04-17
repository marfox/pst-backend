package org.wikidata.query.rdf.primarysources.common;

/**
 * @author Marco Fossati - User:Hjfocs
 * @since 0.2.5
 * Created on Apr 17, 2018.
 */
public class ApiParameters {
    // from /suggest
    public static final String DATASET_PARAMETER = "dataset";
    public static final String QID_PARAMETER = "qid";
    // from /curate
    public static final String STATEMENT_TYPE_JSON_KEY = "type";
    public static final String STATEMENT_STATE_JSON_KEY = "state";
    public static final String QUICKSTATEMENT_JSON_KEY = "qs";
    public static final String DEFAULT_IO_MIME_TYPE = "application/json";
    // from /statistics
    public static final String USER_NAME_PARAMETER = "user";
    // from /upload
    /**
     * Expected HTTP form field with the name of the dataset.
     */
    public static final String DATASET_NAME_FORM_FIELD = "name";
    /**
     * Optional HTTP form field with the dataset description.
     */
    public static final String DATASET_DESCRIPTION_FORM_FIELD = "description";
    // from /update
    /**
     * Required HTML form field: the value should be the file with the dataset to be removed.
     */
    public static final String REMOVE_FORM_FIELD = "remove";
    /**
     * Required HTML form field: the value should be the file with the dataset to be added.
     */
    public static final String ADD_FORM_FIELD = "add";
    /**
     * Query parameter (with no value) to call the Blazegraph update with multi-part request body service.
     * See https://wiki.blazegraph.com/wiki/index.php/REST_API#UPDATE_.28POST_with_Multi-Part_Request_Body.29
     */
    public static final String BLAZEGRAPH_UPDATE_PARAMETER = "updatePost";
    /**
     * Query parameter for the Blazegraph update service: the value should be the URI of the target dataset that undergoes deletion.
     * This is not documented in https://wiki.blazegraph.com/wiki/index.php/REST_API#UPDATE_.28POST_with_Multi-Part_Request_Body.29
     * but can be found in the source code:
     * https://github.com/blazegraph/database/blob/master/bigdata-core/bigdata-sails/src/java/com/bigdata/rdf/sail/webapp/UpdateServlet.java#L896
     */
    public static final String BLAZEGRAPH_UPDATE_DELETE_NAMED_GRAPH_PARAMETER = "context-uri-delete";
    /**
     * Query parameter for the Blazegraph update service: the value should be the URI of the target dataset that undergoes addition.
     * This is not documented in https://wiki.blazegraph.com/wiki/index.php/REST_API#UPDATE_.28POST_with_Multi-Part_Request_Body.29
     * but can be found in the source code:
     * https://github.com/blazegraph/database/blob/master/bigdata-core/bigdata-sails/src/java/com/bigdata/rdf/sail/webapp/UpdateServlet.java#L877
     */
    public static final String BLAZEGRAPH_UPDATE_INSERT_NAMED_GRAPH_PARAMETER = "context-uri-insert";
    // from /search
    public static final String OFFSET_PARAMETER = "offset";
    public static final String LIMIT_PARAMETER = "limit";
    public static final String PROPERTY_PARAMETER = "property";
    public static final String VALUE_PARAMETER = "value";
}
