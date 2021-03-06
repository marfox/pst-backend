package org.wikidata.query.rdf.primarysources.common;

/**
 * A set of SPARQL queries used by the Wikidata primary sources tool.
 * <p>
 * Fields ending with {@code QUERY} are SPARQL query templates.
 * Fields ending with {@code PLACE_HOLDER} serve as place holders inside queries, and are filled with actual values at run time.
 *
 * @author Marco Fossati - <a href="https://meta.wikimedia.org/wiki/User:Hjfocs">User:Hjfocs</a>
 * @since 0.2.5 - created on Apr 17, 2018.
 */
public final class SparqlQueries {

    /* Query place holders */
    public static final String USER_PLACE_HOLDER = "${USER}";
    public static final String STATE_PLACE_HOLDER = "${STATE}";
    public static final String MAIN_PID_PLACE_HOLDER = "${MAIN_PID}";
    public static final String PID_PLACE_HOLDER = "${PID}";
    public static final String VALUE_PLACE_HOLDER = "${VALUE}";
    public static final String DATASET_PLACE_HOLDER = "${DATASET}";
    public static final String PROPERTY_PLACE_HOLDER = "${PROPERTY}";
    public static final String ITEM_VALUE_PLACE_HOLDER = "${ITEM_VALUE}";
    public static final String OFFSET_PLACE_HOLDER = "${OFFSET}";
    public static final String LIMIT_PLACE_HOLDER = "${LIMIT}";
    public static final String QID_PLACE_HOLDER = "${QID}";

    /* BEGIN: Curation API */
    // Used by /curate
    /**
     * Approve a claim, together with eventual qualifiers.
     */
    public static final String CLAIM_APPROVAL_QUERY =
        "DELETE {" +
            "  GRAPH <" + DATASET_PLACE_HOLDER + "/new> {" +
            "    ?claim ps:" + PID_PLACE_HOLDER + " " + VALUE_PLACE_HOLDER + " ;" +
            "           ?pq ?qualifier ." +
            "  }" +
            "  GRAPH <" + RdfVocabulary.METADATA_NAMESPACE + "> {" +
            "    <" + RdfVocabulary.USER_URI_PREFIX + USER_PLACE_HOLDER + "> <" + RdfVocabulary.METADATA_NAMESPACE + "/activities> ?activities ." +
            "  }" +
            "}" +
            "INSERT {" +
            "  GRAPH <" + DATASET_PLACE_HOLDER + "/approved> {" +
            "    ?claim ps:" + PID_PLACE_HOLDER + " " + VALUE_PLACE_HOLDER + " ;" +
            "           ?pq ?qualifier ." +
            "  }" +
            "  GRAPH <" + RdfVocabulary.METADATA_NAMESPACE + "> {" +
            "    <" + RdfVocabulary.USER_URI_PREFIX + USER_PLACE_HOLDER + "> <" + RdfVocabulary.METADATA_NAMESPACE + "/activities> ?incremented ." +
            "  }" +
            "}" +
            "WHERE {" +
            "  GRAPH <" + DATASET_PLACE_HOLDER + "/new> {" +
            "    ?claim ps:" + PID_PLACE_HOLDER + " " + VALUE_PLACE_HOLDER + " ." +
            "    OPTIONAL {" +
            "      ?claim ?pq ?qualifier ." +
            "      FILTER (?pq != prov:wasDerivedFrom) ." +
            "    }" +
            "  }" +
            "  GRAPH <" + RdfVocabulary.METADATA_NAMESPACE + "> {" +
            "    OPTIONAL {" +
            "      <" + RdfVocabulary.USER_URI_PREFIX + USER_PLACE_HOLDER + "> <" + RdfVocabulary.METADATA_NAMESPACE + "/activities> ?activities ." +
            "    }" +
            "    BIND (IF (BOUND (?activities), ?activities + 1, 1) AS ?incremented) ." +
            "  }" +
            "}";
    /**
     * Reject a claim, together with eventual qualifiers and eventual references.
     */
    public static final String CLAIM_REJECTION_QUERY =
        "DELETE {" +
            "  GRAPH <" + DATASET_PLACE_HOLDER + "/new> {" +
            "    wd:" + QID_PLACE_HOLDER + " p:" + MAIN_PID_PLACE_HOLDER + " ?st_node ." +
            "    ?st_node ps:" + PID_PLACE_HOLDER + " " + VALUE_PLACE_HOLDER + " ;" +
            "             prov:wasDerivedFrom ?ref_node ;" +
            "             ?qualif_p ?qualif_v ." +
            "    ?ref_node ?ref_p ?ref_v" +
            "  }" +
            "  GRAPH <" + RdfVocabulary.METADATA_NAMESPACE + "> {" +
            "    <" + RdfVocabulary.USER_URI_PREFIX + USER_PLACE_HOLDER + "> <" + RdfVocabulary.METADATA_NAMESPACE + "/activities> ?activities . " +
            "  }" +
            "}" +
            "INSERT {" +
            "  GRAPH <" + DATASET_PLACE_HOLDER + "/" + STATE_PLACE_HOLDER + "> {" +
            "    wd:" + QID_PLACE_HOLDER + " p:" + MAIN_PID_PLACE_HOLDER + " ?st_node ." +
            "    ?st_node ps:" + PID_PLACE_HOLDER + " " + VALUE_PLACE_HOLDER + " ;" +
            "             prov:wasDerivedFrom ?ref_node ;" +
            "             ?qualif_p ?qualif_v ." +
            "    ?ref_node ?ref_p ?ref_v" +
            "  }" +
            "  GRAPH <" + RdfVocabulary.METADATA_NAMESPACE + "> {" +
            "    <" + RdfVocabulary.USER_URI_PREFIX + USER_PLACE_HOLDER + "> <" + RdfVocabulary.METADATA_NAMESPACE + "/activities> ?incremented ." +
            "  }" +
            "}" +
            "WHERE {" +
            "  GRAPH <" + DATASET_PLACE_HOLDER + "/new> {" +
            "    wd:" + QID_PLACE_HOLDER + " p:" + MAIN_PID_PLACE_HOLDER + " ?st_node ." +
            "    ?st_node ps:" + PID_PLACE_HOLDER + " " + VALUE_PLACE_HOLDER + " ;" +
            "    OPTIONAL {" +
            "      ?st_node prov:wasDerivedFrom ?ref_node ." +
            "      ?ref_node ?ref_p ?ref_v ." +
            "    }" +
            "    OPTIONAL {" +
            "      ?st_node ?qualif_p ?qualif_v ." +
            "    }" +
            "  }" +
            "  GRAPH <" + RdfVocabulary.METADATA_NAMESPACE + "> {" +
            "    OPTIONAL {" +
            "      <" + RdfVocabulary.USER_URI_PREFIX + USER_PLACE_HOLDER + "> <" + RdfVocabulary.METADATA_NAMESPACE + "/activities> ?activities ." +
            "    }" +
            "    BIND (IF (BOUND (?activities), ?activities + 1, 1) AS ?incremented) ." +
            "  }" +
            "}";
    /**
     * Change the state of a reference, together with its main claim and eventual qualifiers.
     * Keep sibling references unchanged.
     */
    public static final String REFERENCE_CURATION_QUERY =
        "DELETE {" +
            "  GRAPH <" + DATASET_PLACE_HOLDER + "/new> {" +
            "    wd:" + QID_PLACE_HOLDER + " p:" + MAIN_PID_PLACE_HOLDER + " ?st_node ." +
            "    ?st_node ps:" + MAIN_PID_PLACE_HOLDER + " ?st_value ;" +
            "             prov:wasDerivedFrom ?ref_node ;" +
            "             ?qualif_p ?qualif_v ." +
            "    ?ref_node pr:" + PID_PLACE_HOLDER + " " + VALUE_PLACE_HOLDER + " ." +
            "  }" +
            "  GRAPH <" + RdfVocabulary.METADATA_NAMESPACE + "> {" +
            "    <" + RdfVocabulary.USER_URI_PREFIX + USER_PLACE_HOLDER + "> <" + RdfVocabulary.METADATA_NAMESPACE + "/activities> ?activities . " +
            "  }" +
            "}" +
            "INSERT {" +
            "  GRAPH <" + DATASET_PLACE_HOLDER + "/" + STATE_PLACE_HOLDER + "> {" +
            "    wd:" + QID_PLACE_HOLDER + " p:" + MAIN_PID_PLACE_HOLDER + " ?st_node ." +
            "    ?st_node ps:" + MAIN_PID_PLACE_HOLDER + " ?st_value ;" +
            "             prov:wasDerivedFrom ?ref_node ;" +
            "             ?qualif_p ?qualif_v ." +
            "    ?ref_node pr:" + PID_PLACE_HOLDER + " " + VALUE_PLACE_HOLDER + " ." +
            "  }" +
            "  GRAPH <" + RdfVocabulary.METADATA_NAMESPACE + "> {" +
            "    <" + RdfVocabulary.USER_URI_PREFIX + USER_PLACE_HOLDER + "> <" + RdfVocabulary.METADATA_NAMESPACE + "/activities> ?incremented ." +
            "  }" +
            "}" +
            "WHERE {" +
            "  GRAPH <" + DATASET_PLACE_HOLDER + "/new> {" +
            "    wd:" + QID_PLACE_HOLDER + " p:" + MAIN_PID_PLACE_HOLDER + " ?st_node ." +
            "    ?st_node ps:" + MAIN_PID_PLACE_HOLDER + " ?st_value ;" +
            "             prov:wasDerivedFrom ?ref_node ." +
            "    ?ref_node pr:" + PID_PLACE_HOLDER + " " + VALUE_PLACE_HOLDER + " ." +
            "    OPTIONAL {" +
            "      ?st_node ?qualif_p ?qualif_v ." +
            "      FILTER (?qualif_p != prov:wasDerivedFrom) ." +
            "     }" +
            "  }" +
            "  GRAPH <" + RdfVocabulary.METADATA_NAMESPACE + "> {" +
            "    OPTIONAL {" +
            "      <" + RdfVocabulary.USER_URI_PREFIX + USER_PLACE_HOLDER + "> <" + RdfVocabulary.METADATA_NAMESPACE + "/activities> ?activities ." +
            "    }" +
            "    BIND (IF (BOUND (?activities), ?activities + 1, 1) AS ?incremented) ." +
            "  }" +
            "}";
    /**
     * Change the state of a qualifier, together with its main claim and eventual references.
     * Keep sibling qualifiers unchanged.
     */
    public static final String QUALIFIER_CURATION_QUERY =
        "DELETE {" +
            "  GRAPH <" + DATASET_PLACE_HOLDER + "/new> {" +
            "    ?st_node ps:" + MAIN_PID_PLACE_HOLDER + " ?st_value ;" +
            "             prov:wasDerivedFrom ?ref_node ." +
            "    ?qualifier pq:" + PID_PLACE_HOLDER + " " + VALUE_PLACE_HOLDER + " ." +
            "  }" +
            "  GRAPH <" + RdfVocabulary.METADATA_NAMESPACE + "> {" +
            "    <" + RdfVocabulary.USER_URI_PREFIX + USER_PLACE_HOLDER + "> <" + RdfVocabulary.METADATA_NAMESPACE + "/activities> ?activities . " +
            "  }" +
            "}" +
            "INSERT {" +
            "  GRAPH <" + DATASET_PLACE_HOLDER + "/" + STATE_PLACE_HOLDER + "> {" +
            "    ?st_node ps:" + MAIN_PID_PLACE_HOLDER + " ?st_value ;" +
            "             prov:wasDerivedFrom ?ref_node ." +
            "    ?qualifier pq:" + PID_PLACE_HOLDER + " " + VALUE_PLACE_HOLDER + " ." +
            "  }" +
            "  GRAPH <" + RdfVocabulary.METADATA_NAMESPACE + "> {" +
            "    <" + RdfVocabulary.USER_URI_PREFIX + USER_PLACE_HOLDER + "> <" + RdfVocabulary.METADATA_NAMESPACE + "/activities> ?incremented ." +
            "  }" +
            "}" +
            "WHERE {" +
            "  GRAPH <" + DATASET_PLACE_HOLDER + "/new> {" +
            "    wd:" + QID_PLACE_HOLDER + " p:" + MAIN_PID_PLACE_HOLDER + " ?st_node ." +
            "    ?st_node ps:" + MAIN_PID_PLACE_HOLDER + " ?st_value ." +
            "    ?qualifier pq:" + PID_PLACE_HOLDER + " " + VALUE_PLACE_HOLDER + " ." +
            "    OPTIONAL {" +
            "      ?st_node prov:wasDerivedFrom ?ref_node ." +
            "      ?ref_node ?ref_p ?ref_v ." +
            "    }" +
            "  }" +
            "  GRAPH <" + RdfVocabulary.METADATA_NAMESPACE + "> {" +
            "    OPTIONAL {" +
            "      <" + RdfVocabulary.USER_URI_PREFIX + USER_PLACE_HOLDER + "> <" + RdfVocabulary.METADATA_NAMESPACE + "/activities> ?activities ." +
            "    }" +
            "    BIND (IF (BOUND (?activities), ?activities + 1, 1) AS ?incremented) ." +
            "  }" +
            "}";

    // Used by /search
    /**
     * Search dataset-specific statements to be curated, filtered by an item value.
     */
    public static final String SEARCH_ONE_DATASET_VALUE_QUERY =
        "SELECT * " +
            "WHERE {" +
            "  GRAPH <" + DATASET_PLACE_HOLDER + "> {" +
            "    ?item a wikibase:Item ;" +
            "      " + PROPERTY_PLACE_HOLDER + " ?statement_node ." +
            "    {" +
            "      SELECT ?statement_node WHERE {" +
            "        ?statement_node ?statement_property wd:" + ITEM_VALUE_PLACE_HOLDER + " ." +
            "      }" +
            "    }" +
            "    ?statement_node ?statement_property ?statement_value ." +
            "    OPTIONAL {" +
            "      ?statement_value ?reference_property ?reference_value ." +
            "    }" +
            "  }" +
            "}" +
            "OFFSET " + OFFSET_PLACE_HOLDER + " " +
            "LIMIT " + LIMIT_PLACE_HOLDER;
    /**
     * Search dataset-specific statements to be curated.
     */
    public static final String SEARCH_ONE_DATASET_QUERY =
        "SELECT * " +
            "WHERE {" +
            "  GRAPH <" + DATASET_PLACE_HOLDER + "> {" +
            "    ?item a wikibase:Item ;" +
            "      " + PROPERTY_PLACE_HOLDER + " ?statement_node ." +
            "    ?statement_node ?statement_property ?statement_value ." +
            "    OPTIONAL {" +
            "      ?statement_value ?reference_property ?reference_value ." +
            "    }" +
            "  }" +
            "}" +
            "OFFSET " + OFFSET_PLACE_HOLDER + " " +
            "LIMIT " + LIMIT_PLACE_HOLDER;
    /**
     * Search all statements to be curated, filtered by an item value.
     */
    public static final String SEARCH_ALL_DATASETS_VALUE_QUERY =
        "SELECT * " +
            "WHERE {" +
            "  GRAPH ?dataset {" +
            "    ?item a wikibase:Item ;" +
            "      " + PROPERTY_PLACE_HOLDER + " ?statement_node ." +
            "    {" +
            "      SELECT ?statement_node WHERE {" +
            "        ?statement_node ?statement_property wd:" + ITEM_VALUE_PLACE_HOLDER + " ." +
            "      }" +
            "    }" +
            "    ?statement_node ?statement_property ?statement_value ." +
            "    OPTIONAL {" +
            "      ?statement_value ?reference_property ?reference_value ." +
            "    }" +
            "  }" +
            "  FILTER STRENDS(str(?dataset), \"new\") ." +
            "}" +
            "OFFSET " + OFFSET_PLACE_HOLDER + " " +
            "LIMIT " + LIMIT_PLACE_HOLDER;
    /**
     * Search all statements to be curated.
     */
    public static final String SEARCH_ALL_DATASETS_QUERY =
        "SELECT * " +
            "WHERE {" +
            "  GRAPH ?dataset {" +
            "    ?item a wikibase:Item ;" +
            "      " + PROPERTY_PLACE_HOLDER + " ?statement_node ." +
            "    ?statement_node ?statement_property ?statement_value ." +
            "    OPTIONAL {" +
            "      ?statement_value ?reference_property ?reference_value ." +
            "    }" +
            "  }" +
            "  FILTER STRENDS(str(?dataset), \"new\") ." +
            "}" +
            "OFFSET " + OFFSET_PLACE_HOLDER + " " +
            "LIMIT " + LIMIT_PLACE_HOLDER;
    // Used by /suggest and /random
    static final String SUGGEST_ALL_DATASETS_QUERY =
        "SELECT ?dataset ?property ?statement_node ?statement_property ?statement_value ?reference_property ?reference_value " +
            "WHERE {" +
            "  GRAPH ?dataset {" +
            "    wd:" + QID_PLACE_HOLDER + " a wikibase:Item ;" +
            "                                ?property ?statement_node ." +
            "    ?statement_node ?statement_property ?statement_value ." +
            "    OPTIONAL {" +
            "      ?statement_value ?reference_property ?reference_value ." +
            "    }" +
            "  }" +
            "  FILTER STRENDS(str(?dataset), \"new\") ." +
            "}";
    static final String SUGGEST_ONE_DATASET_QUERY =
        "SELECT ?property ?statement_node ?statement_property ?statement_value ?reference_property ?reference_value " +
            "WHERE {" +
            "  GRAPH <" + DATASET_PLACE_HOLDER + "> {" +
            "    wd:" + QID_PLACE_HOLDER + " a wikibase:Item ;" +
            "                                ?property ?statement_node ." +
            "    ?statement_node ?statement_property ?statement_value ." +
            "    OPTIONAL {" +
            "      ?statement_value ?reference_property ?reference_value ." +
            "    }" +
            "  }" +
            "}";
    /* END: Curation API */


    /* BEGIN: Statistics API */
    // Used by /statistics
    /**
     * Get the count of user curation activities.
     */
    public static final String USER_INFO_QUERY =
        "SELECT ?activities " +
            "WHERE {" +
            "  GRAPH <" + RdfVocabulary.METADATA_NAMESPACE + "> {" +
            "    <" + RdfVocabulary.USER_URI_PREFIX + USER_PLACE_HOLDER + "> <" + RdfVocabulary.METADATA_NAMESPACE + "/activities> ?activities ." +
            "  }" +
            "}";
    /**
     * Get the dataset uploader Wiki user name and the eventual dataset description.
     */
    public static final String DATASET_INFO_QUERY =
        "SELECT ?description_or_uploader " +
            "WHERE {" +
            "  GRAPH <" + RdfVocabulary.METADATA_NAMESPACE + "> {" +
            "    <" + DATASET_PLACE_HOLDER + "> ?p ?description_or_uploader ." +
            "  }" +
            "}";
    /* END: Statistics API */

    /* BEGIN: datasets statistics cache */
    static final String REFERENCES_COUNT_QUERY = "select ?graph (count(?reference) as ?count) where { graph ?graph { ?statement prov:wasDerivedFrom " +
        "?reference } } group by ?graph";
    static final String STATEMENTS_COUNT_QUERY = "select ?graph (count(?statement) as ?count) where { graph ?graph { ?entity ?property ?statement . FILTER " +
        "STRSTARTS(str(?statement), \"http://www.wikidata.org/entity/statement/\") . } } group by ?graph";
    /* END: datasets statistics cache */

    /* BEGIN: entities cache */
    // Also include qualifier values
    static final String VALUES_ONE_DATASET_QUERY =
        "SELECT DISTINCT ?value " +
            "WHERE {" +
            "  GRAPH <" + DATASET_PLACE_HOLDER + "> {" +
            "    ?subject a wikibase:Item ;" +
            "      ?property ?st_node ." +
            "    ?st_node ?st_property ?value ." +
            "  }" +
            "  FILTER STRSTARTS(str(?value), \"" + Utils.WIKIBASE_URIS.entity() + "Q\") ." +
            "}";
    // Only consider main PIDs, not qualifiers or references
    static final String PROPERTIES_ONE_DATASET_QUERY =
        "SELECT DISTINCT ?property " +
            "WHERE {" +
            "  GRAPH <" + DATASET_PLACE_HOLDER + "> {" +
            "    ?subject a wikibase:Item ;" +
            "      ?property ?statement_node ." +
            "  }" +
            "}";
    // A single query for subjects, properties, and values is too heavy, so split into 3
    static final String SUBJECTS_ONE_DATASET_QUERY =
        "SELECT ?subject " + // No need for a DISTINCT here, one dataset should not have duplicate subjects
            "WHERE {" +
            "  GRAPH <" + DATASET_PLACE_HOLDER + "> {" +
            "    ?subject a wikibase:Item ;" +
            "      ?property ?statement_node ." +
            "  }" +
            "}";
    static final String SUBJECTS_ALL_DATASETS_QUERY =
        "SELECT DISTINCT ?subject ?dataset " +
            "WHERE {" +
            "  GRAPH ?dataset {" +
            "    ?subject a wikibase:Item ;" +
            "      ?property ?statement_node ." +
            "  }" +
            "  FILTER STRENDS(str(?dataset), \"new\") ." +
            "}";
    static final String PROPERTIES_ALL_DATASETS_QUERY =
        "SELECT DISTINCT ?property ?dataset " +
            "WHERE {" +
            "  GRAPH ?dataset {" +
            "    ?subject a wikibase:Item ;" +
            "      ?property ?statement_node ." +
            "  }" +
            "  FILTER STRENDS(str(?dataset), \"new\") ." +
            "}";
    static final String VALUES_ALL_DATASETS_QUERY =
        "SELECT DISTINCT ?value ?dataset " +
            "WHERE {" +
            "  GRAPH ?dataset {" +
            "    ?subject a wikibase:Item ;" +
            "      ?property ?st_node ." +
            "    ?st_node ?st_property ?value ." +
            "  }" +
            "  FILTER STRSTARTS(str(?value), \"" + Utils.WIKIBASE_URIS.entity() + "Q\") ." +
            "  FILTER STRENDS(str(?dataset), \"new\") ." +
            "}";
    /* END: entities cache */

    private SparqlQueries() {
    }

}
