package org.wikidata.query.rdf.primarysources.common;

/**
 * A set of HTTP request parameter names used by the Wikidata primary sources tool Web services.
 * Grouped as follows:
 * <ul>
 * <li>required for clients;</li>
 * <li>optional for clients;</li>
 * <li>internally required to communicate with Blazegraph.</li>
 * </ul>
 *
 * @author Marco Fossati - <a href="https://meta.wikimedia.org/wiki/User:Hjfocs">User:Hjfocs</a>
 * @since 0.2.5 - created on Apr 17, 2018.
 */
public final class ApiParameters {

    /**
     * Required query parameter. Expected value: Wikidata identifier (QID). Used in
     * {@link org.wikidata.query.rdf.primarysources.curation.CurateServlet},
     * {@link org.wikidata.query.rdf.primarysources.curation.SuggestServlet}.
     */
    public static final String QID_PARAMETER = "qid";
    /**
     * Required query parameter. Expected value: Wiki user name. Used in
     * {@link org.wikidata.query.rdf.primarysources.curation.CurateServlet},
     * {@link org.wikidata.query.rdf.primarysources.statistics.StatisticsServlet},
     * {@link org.wikidata.query.rdf.primarysources.ingestion.UpdateServlet},
     * {@link org.wikidata.query.rdf.primarysources.ingestion.UploadServlet}.
     */
    public static final String USER_NAME_PARAMETER = "user";
    /**
     * Required request body JSON key. Expected value: one of {@code reference}, {@code claim}, {@code qualifier}. Used in
     * {@link org.wikidata.query.rdf.primarysources.curation.CurateServlet}.
     */
    public static final String STATEMENT_TYPE_JSON_KEY = "type";
    /**
     * Required request body JSON key. Expected value: one of {@code approved}, {@code rejected}, {@code duplicate}, {@code blacklisted}. Used in
     * {@link org.wikidata.query.rdf.primarysources.curation.CurateServlet}.
     */
    public static final String STATEMENT_STATE_JSON_KEY = "state";
    /**
     * Required request body JSON key. Expected value: <a href="https://www.wikidata.org/wiki/Help:QuickStatements#Command_sequence_syntax">QuickStatement</a>.
     * Used in {@link org.wikidata.query.rdf.primarysources.curation.CurateServlet}.
     */
    public static final String QUICKSTATEMENT_JSON_KEY = "qs";
    /**
     * Required form field. Expected value: human-readable dataset name.
     */
    public static final String DATASET_NAME_FORM_FIELD = "name";
    /**
     * Required form field. Expected value: file with the dataset to be removed.
     */
    public static final String REMOVE_FORM_FIELD = "remove";
    /**
     * Required form field. Expected value: file with the dataset to be added.
     */
    public static final String ADD_FORM_FIELD = "add";
    /**
     * Optional query parameter. Expected value: dataset URI. Used in
     * {@link org.wikidata.query.rdf.primarysources.curation.CurateServlet},
     * {@link org.wikidata.query.rdf.primarysources.curation.RandomServlet},
     * {@link org.wikidata.query.rdf.primarysources.curation.SearchServlet},
     * {@link org.wikidata.query.rdf.primarysources.curation.SuggestServlet}.
     */
    public static final String DATASET_PARAMETER = "dataset";
    /**
     * Optional query parameter. Expected value: offset number.
     */
    public static final String OFFSET_PARAMETER = "offset";
    /**
     * Optional query parameter. Expected value: limit number.
     */
    public static final String LIMIT_PARAMETER = "limit";
    /**
     * Optional query parameter. Expected value: Wikidata property identifier (PID).
     */
    public static final String PROPERTY_PARAMETER = "property";
    /**
     * Optional query parameter. Expected value: Wikidata identifier (QID).
     */
    public static final String VALUE_PARAMETER = "value";
    /**
     * Optional form field. Expected value: dataset description.
     */
    public static final String DATASET_DESCRIPTION_FORM_FIELD = "description";
    /**
     * Internally required query parameter for the Blazegraph
     * <a href="https://wiki.blazegraph.com/wiki/index.php/REST_API#UPDATE_.28POST_with_Multi-Part_Request_Body.29">update with multi-part request body</a>
     * service. Expected value: none.
     */
    public static final String BLAZEGRAPH_UPDATE_PARAMETER = "updatePost";
    /**
     * Internally required query parameter for the Blazegraph update service. Expected value: URI of the target dataset that undergoes deletion.
     * This is <b><a href="https://wiki.blazegraph.com/wiki/index.php/REST_API#UPDATE_.28POST_with_Multi-Part_Request_Body.29">not documented</a></b>,
     * but can be found in the
     * <a href="https://github.com/blazegraph/database/blob/master/bigdata-core/bigdata-sails/src/java/com/bigdata/rdf/sail/webapp/UpdateServlet.java#L896"
     * target="_blank">source code</a>.
     */
    public static final String BLAZEGRAPH_UPDATE_DELETE_NAMED_GRAPH_PARAMETER = "context-uri-delete";
    /**
     * Internally required query parameter for the Blazegraph update service. Expected value: URI of the target dataset that undergoes addition.
     * This is <b><a href="https://wiki.blazegraph.com/wiki/index.php/REST_API#UPDATE_.28POST_with_Multi-Part_Request_Body.29">not documented</a></b>,
     * but can be found in the
     * <a href="https://github.com/blazegraph/database/blob/master/bigdata-core/bigdata-sails/src/java/com/bigdata/rdf/sail/webapp/UpdateServlet.java#L877"
     * target="_blank">source code</a>.
     */
    public static final String BLAZEGRAPH_UPDATE_INSERT_NAMED_GRAPH_PARAMETER = "context-uri-insert";
    /**
     * Default content type used in request and response headers.
     */
    public static final String DEFAULT_IO_CONTENT_TYPE = "application/json";

    private ApiParameters() {
    }
}
