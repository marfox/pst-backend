package org.wikidata.query.rdf.primarysources.curation;

import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Form;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.utils.URIBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.openrdf.model.Literal;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.WikibaseDate;
import org.wikidata.query.rdf.common.WikibasePoint;
import org.wikidata.query.rdf.common.uri.GeoSparql;
import org.wikidata.query.rdf.primarysources.common.WikibaseDataModelValidator;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.wikidata.query.rdf.primarysources.ingestion.UploadServlet.*;

/**
 * @author Marco Fossati - User:Hjfocs
 * @since 0.2.4
 * Created on Aug 30, 2017.
 */
public class CurateServlet extends HttpServlet {

    public static final String USER_PLACE_HOLDER = "${USER}";
    public static final String STATE_PLACE_HOLDER = "${STATE}";
    private static final String MW_API_OBJECT = "for_mw_api";
    private static final String PID_KEY = "property";
    private static final String MAIN_PID_KEY = "main_property";
    private static final String VALUE_KEY = "value";
    private static final String STATEMENT_TYPE_KEY = "type";
    private static final String STATE_KEY = "state";
    private static final String USER_KEY = "user";
    private static final String QUICKSTATEMENT_KEY = "qs";
    private static final String MAIN_PID_PLACE_HOLDER = "${MAIN_PID}";
    private static final String PID_PLACE_HOLDER = "${PID}";
    private static final String VALUE_PLACE_HOLDER = "${VALUE}";
    private static final WikibaseDataModelValidator VALIDATOR = new WikibaseDataModelValidator();
    // Value data types matchers
    private static final Pattern TIME = Pattern.compile("^[+-]\\d+-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\dZ/\\d+$");
    private static final Pattern LOCATION = Pattern.compile("^@([+\\-]?\\d+(?:.\\d+)?)/([+\\-]?\\d+(?:.\\d+))?$");
    private static final Pattern QUANTITY = Pattern.compile("^[+-]\\d+(\\.\\d+)?$");
    private static final Pattern MONOLINGUAL_TEXT = Pattern.compile("^(\\w+):(\"[^\"\\\\]*(?:\\\\.[^\"\\\\]*)*\")$");

    // Approve claim + eventual qualifiers
    private static final String CLAIM_APPROVAL_QUERY =
            "DELETE {" +
                    "  GRAPH <" + SuggestServlet.DATASET_PLACE_HOLDER + "/new> {" +
                    "    ?claim ps:" + PID_PLACE_HOLDER + " " + VALUE_PLACE_HOLDER + " ;" +
                    "           ?pq ?qualifier ." +
                    "  }" +
                    "  GRAPH <" + METADATA_NAMESPACE + "> {" +
                    "    <" + USER_URI_PREFIX + USER_PLACE_HOLDER + "> <" + METADATA_NAMESPACE + "/activities> ?activities ." +
                    "  }" +
                    "}" +
                    "INSERT {" +
                    "  GRAPH <" + SuggestServlet.DATASET_PLACE_HOLDER + "/approved> {" +
                    "    ?claim ps:" + PID_PLACE_HOLDER + " " + VALUE_PLACE_HOLDER + " ;" +
                    "           ?pq ?qualifier ." +
                    "  }" +
                    "  GRAPH <" + METADATA_NAMESPACE + "> {" +
                    "    <" + USER_URI_PREFIX + USER_PLACE_HOLDER + "> <" + METADATA_NAMESPACE + "/activities> ?incremented ." +
                    "  }" +
                    "}" +
                    "WHERE {" +
                    "  GRAPH <" + SuggestServlet.DATASET_PLACE_HOLDER + "/new> {" +
                    "    ?claim ps:" + PID_PLACE_HOLDER + " " + VALUE_PLACE_HOLDER + " ." +
                    "    OPTIONAL {" +
                    "      ?claim ?pq ?qualifier ." +
                    "      FILTER (?pq != prov:wasDerivedFrom) ." +
                    "    }" +
                    "  }" +
                    "  GRAPH <" + METADATA_NAMESPACE + "> {" +
                    "    OPTIONAL {" +
                    "      <" + USER_URI_PREFIX + USER_PLACE_HOLDER + "> <" + METADATA_NAMESPACE + "/activities> ?activities ." +
                    "    }" +
                    "    BIND (IF (BOUND (?activities), ?activities + 1, 1) AS ?incremented) ." +
                    "  }" +
                    "}";
    // Reject everything. Note that the state may be one of 'rejected', 'duplicate', or 'blacklisted'
    private static final String CLAIM_REJECTION_QUERY =
            "DELETE {" +
                    "  GRAPH <" + SuggestServlet.DATASET_PLACE_HOLDER + "/new> {" +
                    "    wd:" + SuggestServlet.QID_PLACE_HOLDER + " p:" + MAIN_PID_PLACE_HOLDER + " ?st_node ." +
                    "    ?st_node ps:" + PID_PLACE_HOLDER + " " + VALUE_PLACE_HOLDER + " ;" +
                    "             prov:wasDerivedFrom ?ref_node ;" +
                    "             ?qualif_p ?qualif_v ." +
                    "    ?ref_node ?ref_p ?ref_v" +
                    "  }" +
                    "  GRAPH <" + METADATA_NAMESPACE + "> {" +
                    "    <" + USER_URI_PREFIX + USER_PLACE_HOLDER + "> <" + METADATA_NAMESPACE + "/activities> ?activities . " +
                    "  }" +
                    "}" +
                    "INSERT {" +
                    "  GRAPH <" + SuggestServlet.DATASET_PLACE_HOLDER + "/" + STATE_PLACE_HOLDER + "> {" +
                    "    wd:" + SuggestServlet.QID_PLACE_HOLDER + " p:" + MAIN_PID_PLACE_HOLDER + " ?st_node ." +
                    "    ?st_node ps:" + PID_PLACE_HOLDER + " " + VALUE_PLACE_HOLDER + " ;" +
                    "             prov:wasDerivedFrom ?ref_node ;" +
                    "             ?qualif_p ?qualif_v ." +
                    "    ?ref_node ?ref_p ?ref_v" +
                    "  }" +
                    "  GRAPH <" + METADATA_NAMESPACE + "> {" +
                    "    <" + USER_URI_PREFIX + USER_PLACE_HOLDER + "> <" + METADATA_NAMESPACE + "/activities> ?incremented ." +
                    "  }" +
                    "}" +
                    "WHERE {" +
                    "  GRAPH <" + SuggestServlet.DATASET_PLACE_HOLDER + "/new> {" +
                    "    wd:" + SuggestServlet.QID_PLACE_HOLDER + " p:" + MAIN_PID_PLACE_HOLDER + " ?st_node ." +
                    "    ?st_node ps:" + PID_PLACE_HOLDER + " " + VALUE_PLACE_HOLDER + " ;" +
                    "    OPTIONAL {" +
                    "      ?st_node prov:wasDerivedFrom ?ref_node ." +
                    "      ?ref_node ?ref_p ?ref_v ." +
                    "    }" +
                    "    OPTIONAL {" +
                    "      ?st_node ?qualif_p ?qualif_v ." +
                    "    }" +
                    "  }" +
                    "  GRAPH <" + METADATA_NAMESPACE + "> {" +
                    "    OPTIONAL {" +
                    "      <" + USER_URI_PREFIX + USER_PLACE_HOLDER + "> <" + METADATA_NAMESPACE + "/activities> ?activities ." +
                    "    }" +
                    "    BIND (IF (BOUND (?activities), ?activities + 1, 1) AS ?incremented) ." +
                    "  }" +
                    "}";

    // Approve/reject everything but main node + sibling qualifiers
    private static final String QUALIFIER_QUERY =
            "DELETE {" +
                    "  GRAPH <" + SuggestServlet.DATASET_PLACE_HOLDER + "/new> {" +
                    "    ?st_node ps:" + MAIN_PID_PLACE_HOLDER + " ?st_value ;" +
                    "             prov:wasDerivedFrom ?ref_node ." +
                    "    ?qualifier pq:" + PID_PLACE_HOLDER + " " + VALUE_PLACE_HOLDER + " ." +
                    "  }" +
                    "  GRAPH <" + METADATA_NAMESPACE + "> {" +
                    "    <" + USER_URI_PREFIX + USER_PLACE_HOLDER + "> <" + METADATA_NAMESPACE + "/activities> ?activities . " +
                    "  }" +
                    "}" +
                    "INSERT {" +
                    "  GRAPH <" + SuggestServlet.DATASET_PLACE_HOLDER + "/" + STATE_PLACE_HOLDER + "> {" +
                    "    ?st_node ps:" + MAIN_PID_PLACE_HOLDER + " ?st_value ;" +
                    "             prov:wasDerivedFrom ?ref_node ." +
                    "    ?qualifier pq:" + PID_PLACE_HOLDER + " " + VALUE_PLACE_HOLDER + " ." +
                    "  }" +
                    "  GRAPH <" + METADATA_NAMESPACE + "> {" +
                    "    <" + USER_URI_PREFIX + USER_PLACE_HOLDER + "> <" + METADATA_NAMESPACE + "/activities> ?incremented ." +
                    "  }" +
                    "}" +
                    "WHERE {" +
                    "  GRAPH <" + SuggestServlet.DATASET_PLACE_HOLDER + "/new> {" +
                    "    wd:" + SuggestServlet.QID_PLACE_HOLDER + " p:" + MAIN_PID_PLACE_HOLDER + " ?st_node ." +
                    "    ?st_node ps:" + MAIN_PID_PLACE_HOLDER + " ?st_value ." +
                    "    ?qualifier pq:" + PID_PLACE_HOLDER + " " + VALUE_PLACE_HOLDER + " ." +
                    "    OPTIONAL {" +
                    "      ?st_node prov:wasDerivedFrom ?ref_node ." +
                    "      ?ref_node ?ref_p ?ref_v ." +
                    "    }" +
                    "  }" +
                    "  GRAPH <" + METADATA_NAMESPACE + "> {" +
                    "    OPTIONAL {" +
                    "      <" + USER_URI_PREFIX + USER_PLACE_HOLDER + "> <" + METADATA_NAMESPACE + "/activities> ?activities ." +
                    "    }" +
                    "    BIND (IF (BOUND (?activities), ?activities + 1, 1) AS ?incremented) ." +
                    "  }" +
                    "}";

    // Approve/reject everything but sibling references
    private static final String REFERENCE_QUERY =
            "DELETE {" +
                    "  GRAPH <" + SuggestServlet.DATASET_PLACE_HOLDER + "/new> {" +
                    "    wd:" + SuggestServlet.QID_PLACE_HOLDER + " p:" + MAIN_PID_PLACE_HOLDER + " ?st_node ." +
                    "    ?st_node ps:" + MAIN_PID_PLACE_HOLDER + " ?st_value ;" +
                    "             prov:wasDerivedFrom ?ref_node ;" +
                    "             ?qualif_p ?qualif_v ." +
                    "    ?ref_node pr:" + PID_PLACE_HOLDER + " " + VALUE_PLACE_HOLDER + " ." +
                    "  }" +
                    "  GRAPH <" + METADATA_NAMESPACE + "> {" +
                    "    <" + USER_URI_PREFIX + USER_PLACE_HOLDER + "> <" + METADATA_NAMESPACE + "/activities> ?activities . " +
                    "  }" +
                    "}" +
                    "INSERT {" +
                    "  GRAPH <" + SuggestServlet.DATASET_PLACE_HOLDER + "/" + STATE_PLACE_HOLDER + "> {" +
                    "    wd:" + SuggestServlet.QID_PLACE_HOLDER + " p:" + MAIN_PID_PLACE_HOLDER + " ?st_node ." +
                    "    ?st_node ps:" + MAIN_PID_PLACE_HOLDER + " ?st_value ;" +
                    "             prov:wasDerivedFrom ?ref_node ;" +
                    "             ?qualif_p ?qualif_v ." +
                    "    ?ref_node pr:" + PID_PLACE_HOLDER + " " + VALUE_PLACE_HOLDER + " ." +
                    "  }" +
                    "  GRAPH <" + METADATA_NAMESPACE + "> {" +
                    "    <" + USER_URI_PREFIX + USER_PLACE_HOLDER + "> <" + METADATA_NAMESPACE + "/activities> ?incremented ." +
                    "  }" +
                    "}" +
                    "WHERE {" +
                    "  GRAPH <" + SuggestServlet.DATASET_PLACE_HOLDER + "/new> {" +
                    "    wd:" + SuggestServlet.QID_PLACE_HOLDER + " p:" + MAIN_PID_PLACE_HOLDER + " ?st_node ." +
                    "    ?st_node ps:" + MAIN_PID_PLACE_HOLDER + " ?st_value ;" +
                    "             prov:wasDerivedFrom ?ref_node ." +
                    "    ?ref_node pr:" + PID_PLACE_HOLDER + " " + VALUE_PLACE_HOLDER + " ." +
                    "    OPTIONAL {" +
                    "      ?st_node ?qualif_p ?qualif_v ." +
                    "      FILTER (?qualif_p != prov:wasDerivedFrom) ." +
                    "     }" +
                    "  }" +
                    "  GRAPH <" + METADATA_NAMESPACE + "> {" +
                    "    OPTIONAL {" +
                    "      <" + USER_URI_PREFIX + USER_PLACE_HOLDER + "> <" + METADATA_NAMESPACE + "/activities> ?activities ." +
                    "    }" +
                    "    BIND (IF (BOUND (?activities), ?activities + 1, 1) AS ?incremented) ." +
                    "  }" +
                    "}";

    private static final Logger log = LoggerFactory.getLogger(CurateServlet.class);

    private class RequestParameters {
        private String qId;
        private String pId;
        private String mainPId;
        private Value value;
        private String type;
        private String state;
        private String user;
        private String dataset;
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        RequestParameters parameters = new RequestParameters();
        boolean ok = processQuickStatementRequest(request, parameters, response);
        if (!ok) return;
        JSONObject blazegraphError = changeState(parameters);
        sendResponse(response, blazegraphError);
        log.info("POST /curate successful");
    }

    private boolean processQuickStatementRequest(HttpServletRequest request, RequestParameters parameters, HttpServletResponse response) throws IOException {
        JSONObject body;
        try (BufferedReader requestReader = request.getReader()) {
            JSONParser parser = new JSONParser();
            body = (JSONObject) parser.parse(requestReader);
        } catch (ParseException pe) {
            log.warn("Malformed JSON request body. Parse error at index {}, reason: {}", pe.getPosition(), pe.getUnexpectedObject());
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Malformed JSON request body. Parse error at index "
                    + pe.getPosition() + ", reason: " + pe.getUnexpectedObject().toString());
            return false;
        }
        String givenState = (String) body.get(STATE_KEY);
        if (givenState == null) {
            log.warn("No state given. Will fail with a bad request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing required state. Must be one of 'approved', 'rejected', 'duplicate', or 'blacklisted'.");
            return false;
        } else if (!givenState.equals("approved") && !givenState.equals("rejected") && !givenState.equals("duplicate") && !givenState.equals("blacklisted")) {
            log.warn("Invalid statement state: {}. Must be one of 'approved', 'rejected', 'duplicate', or 'blacklisted'. Will fail with a bad request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid statement state: '" + givenState + "'. " +
                    "Must be one of 'approved', 'rejected', 'duplicate', or 'blacklisted'.");
            return false;
        }
        parameters.state = givenState;
        String givenUser = (String) body.get(USER_KEY);
        if (givenUser == null) {
            log.warn("No user name given. Will fail with a bad request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing required user name.");
            return false;
        }
        parameters.user = givenUser;
        String givenDataset = (String) body.get(SuggestServlet.DATASET_PARAMETER);
        if (givenDataset == null) {
            log.warn("No dataset URI given. Will fail with a bad request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing required dataset URI.");
            return false;
        }
        try {
            new URI(givenDataset);
        } catch (URISyntaxException use) {
            log.warn("Invalid dataset URI: {}. Parse error at index {}. Will fail with a bad request", use.getInput(), use.getIndex());
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid dataset URI: <" + use.getInput() + ">. " +
                    "Parse error at index " + use.getIndex() + ".");
            return false;
        }
        parameters.dataset = givenDataset.replace("/new", "");
        String givenType = (String) body.get(STATEMENT_TYPE_KEY);
        if (givenType == null) {
            log.warn("No statement type (claim | qualifier | reference) given. Will fail with a bad request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing required statement type, one of 'claim', 'qualifier', 'reference'.");
            return false;
        } else if (!givenType.equals("claim") && !givenType.equals("qualifier") && !givenType.equals("reference")) {
            log.warn("Invalid statement type: {}. Must be one of 'claim', 'qualifier', 'reference'. Will fail with a bad request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid statement type: '" + givenType + "'. " +
                    "Must be one of 'claim', 'qualifier', 'reference'.");
            return false;
        }
        parameters.type = givenType;
        String givenQuickStatement = (String) body.get(QUICKSTATEMENT_KEY);
        if (givenQuickStatement == null) {
            log.warn("No QuickStatement given. Will fail with a bad request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing required QuickStatement.");
            return false;
        }
        boolean parsed = parseQuickStatement(givenQuickStatement, parameters);
        if (!parsed) {
            log.warn("Could not parse the given QuickStatement. Will fail with a bad request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Malformed QuickStatement: " + givenQuickStatement);
            return false;
        }
        log.debug("Required parameters stored as fields in private class: {}", parameters);
        return true;
    }

    private boolean processMwApiBodyRequest(HttpServletRequest request, RequestParameters parameters, HttpServletResponse response) throws IOException {
        JSONObject body;
        try (BufferedReader requestReader = request.getReader()) {
            JSONParser parser = new JSONParser();
            body = (JSONObject) parser.parse(requestReader);
        } catch (ParseException pe) {
            log.error("Malformed JSON request body. Parse error at index {}, reason: {}", pe.getPosition(), pe.getUnexpectedObject());
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Malformed JSON request body. Parse error at index "
                    + pe.getPosition() + ", reason: " + pe.getUnexpectedObject().toString());
            return false;
        }
        String givenQId = (String) body.get(SuggestServlet.QID_PARAMETER);
        if (givenQId == null) {
            log.error("No QID given. Will fail with a bad request.");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing required QID.");
            return false;
        } else if (!VALIDATOR.isValidTerm(givenQId, "item")) {
            log.error("Invalid QID: {}. Will fail with a bad request.", givenQId);
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid QID: '" + givenQId + "'");
            return false;
        }
        parameters.qId = givenQId;
        String givenMainPId = (String) body.get(MAIN_PID_KEY);
        if (givenMainPId == null) {
            log.error("No main PID given. will fail with a bad request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing required main PID.");
            return false;
        } else if (!VALIDATOR.isValidTerm(givenMainPId, "property")) {
            log.error("Invalid main PID: {}. Will fail with a bad request.", givenMainPId);
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid main PID: '" + givenMainPId + "'");
            return false;
        }
        parameters.mainPId = givenMainPId;
        JSONObject mwApiObject = (JSONObject) body.get(MW_API_OBJECT);
        Object jsonValue;
        Value rdfValue;
        if (mwApiObject.containsKey("snaks")) {
            JSONObject snaks = (JSONObject) mwApiObject.get("snaks");
            Object[] pIds = snaks.keySet().toArray();
            // There is only one key in this set, see SuggestServlet#handleReference
            String givenPId = (String) pIds[0];
            if (givenPId == null) {
                log.error("No PID given. will fail with a bad request");
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing required PID.");
                return false;
            } else if (!VALIDATOR.isValidTerm(givenPId, "property")) {
                log.error("Invalid PID: {}. Will fail with a bad request.", givenPId);
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid PID: '" + givenPId + "'");
                return false;
            }
            parameters.pId = givenPId;
            JSONArray values = (JSONArray) snaks.get(givenPId);
            // There is only one element in this array, see SuggestServlet#handleReference
            jsonValue = values.get(0);
            if (jsonValue == null) {
                log.error("No data value given. will fail with a bad request");
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing required data value.");
                return false;
            }
            rdfValue = wikidataJsonReferenceValueToRdf(jsonValue);
        } else {
            String givenPId = (String) mwApiObject.get(PID_KEY);
            if (givenPId == null) {
                log.error("No PID given. will fail with a bad request");
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing required PID.");
                return false;
            } else if (!VALIDATOR.isValidTerm(givenPId, "property")) {
                log.error("Invalid PID: {}. Will fail with a bad request.", givenPId);
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid PID: '" + givenPId + "'");
                return false;
            }
            parameters.pId = givenPId;
            jsonValue = mwApiObject.get(VALUE_KEY);
            if (jsonValue == null) {
                log.error("No data value given. will fail with a bad request");
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing required data value.");
                return false;
            }
            rdfValue = wikidataJsonValueToRdf(jsonValue);
        }
        if (rdfValue == null) {
            log.error("Unexpected JSON value: {}, Will fail with a bad request", jsonValue);
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Unexpected JSON value: '" + jsonValue + "'. Expected an object or a string");
            return false;
        }
        parameters.value = rdfValue;
        String givenType = (String) body.get(STATEMENT_TYPE_KEY);
        if (givenType == null) {
            log.error("No statement type (claim | qualifier | reference) given. Will fail with a bad request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing required statement type, one of 'claim', 'qualifier', 'reference'.");
            return false;
        } else if (!givenType.equals("claim") && !givenType.equals("qualifier") && !givenType.equals("reference")) {
            log.error("Invalid statement type: {}. Must be one of 'claim', 'qualifier', 'reference'. Will fail with a bad request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid statement type: '" + givenType + "'. " +
                    "Must be one of 'claim', 'qualifier', 'reference'.");
            return false;
        }
        parameters.type = givenType;
        String givenState = (String) body.get(STATE_KEY);
        if (givenState == null) {
            log.error("No state given. Will fail with a bad request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing required state, either 'approved' or 'rejected'.");
            return false;
        } else if (!givenState.equals("approved") && !givenState.equals("rejected")) {
            log.error("Invalid statement state: {}. Must be either 'approved' or 'rejected'. Will fail with a bad request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid statement state: '" + givenState + "'. " +
                    "Must be either 'approved' or 'rejected'.");
            return false;
        }
        parameters.state = givenState;
        String givenUser = (String) body.get(USER_KEY);
        if (givenUser == null) {
            log.error("No user name given. Will fail with a bad request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing required user name.");
            return false;
        }
        parameters.user = givenUser;
        String givenDataset = (String) body.get(SuggestServlet.DATASET_PARAMETER);
        if (givenDataset == null) {
            log.error("No dataset URI given. Will fail with a bad request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing required dataset URI.");
            return false;
        }
        try {
            new URI(givenDataset);
        } catch (URISyntaxException use) {
            log.error("Invalid dataset URI: {}. Parse error at index {}. Will fail with a bad request", use.getInput(), use.getIndex());
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid dataset URI: <" + use.getInput() + ">. " +
                    "Parse error at index " + use.getIndex() + ".");
            return false;
        }
        parameters.dataset = givenDataset.replace("/new", "");
        return true;
    }

    private JSONObject changeState(RequestParameters parameters) throws IOException {
        String query = null;
        switch (parameters.type) {
            case "claim":
                query = parameters.state.equals("approved") ? CLAIM_APPROVAL_QUERY : CLAIM_REJECTION_QUERY;
                query = query
                        .replace(USER_PLACE_HOLDER, parameters.user)
                        .replace(SuggestServlet.DATASET_PLACE_HOLDER, parameters.dataset)
                        .replace(STATE_PLACE_HOLDER, parameters.state)
                        .replace(SuggestServlet.QID_PLACE_HOLDER, parameters.qId)
                        .replace(MAIN_PID_PLACE_HOLDER, parameters.mainPId)
                        .replace(PID_PLACE_HOLDER, parameters.pId);
                query = parameters.value instanceof org.openrdf.model.URI
                        ? query.replace(VALUE_PLACE_HOLDER, "<" + parameters.value.toString() + ">")
                        : query.replace(VALUE_PLACE_HOLDER, parameters.value.toString());
                break;
            case "qualifier":
                query = QUALIFIER_QUERY
                        .replace(USER_PLACE_HOLDER, parameters.user)
                        .replace(SuggestServlet.DATASET_PLACE_HOLDER, parameters.dataset)
                        .replace(STATE_PLACE_HOLDER, parameters.state)
                        .replace(SuggestServlet.QID_PLACE_HOLDER, parameters.qId)
                        .replace(MAIN_PID_PLACE_HOLDER, parameters.mainPId)
                        .replace(PID_PLACE_HOLDER, parameters.pId);
                query = parameters.value instanceof org.openrdf.model.URI
                        ? query.replace(VALUE_PLACE_HOLDER, "<" + parameters.value.toString() + ">")
                        : query.replace(VALUE_PLACE_HOLDER, parameters.value.toString());
                break;
            case "reference":
                query = REFERENCE_QUERY
                        .replace(USER_PLACE_HOLDER, parameters.user)
                        .replace(SuggestServlet.DATASET_PLACE_HOLDER, parameters.dataset)
                        .replace(STATE_PLACE_HOLDER, parameters.state)
                        .replace(SuggestServlet.QID_PLACE_HOLDER, parameters.qId)
                        .replace(MAIN_PID_PLACE_HOLDER, parameters.mainPId)
                        .replace(PID_PLACE_HOLDER, parameters.pId);
                query = parameters.value instanceof org.openrdf.model.URI
                        ? query.replace(VALUE_PLACE_HOLDER, "<" + parameters.value.toString() + ">")
                        : query.replace(VALUE_PLACE_HOLDER, parameters.value.toString());
                break;
        }
        log.debug("SPARQL update query to be sent to Blazegraph: {}", query);
        URIBuilder builder = new URIBuilder();
        URI uri;
        try {
            uri = builder
                    .setScheme("http")
                    .setHost(BLAZEGRAPH_HOST)
                    .setPort(BLAZEGRAPH_PORT)
                    .setPath(BLAZEGRAPH_CONTEXT + BLAZEGRAPH_SPARQL_ENDPOINT)
                    .build();
        } catch (URISyntaxException use) {
            log.error("Failed building the URI to query Blazegraph: {}. Parse error at index {}", use.getInput(), use.getIndex());
            JSONObject toBeReturned = new JSONObject();
            toBeReturned.put("error_message", "Failed building the URI to query Blazegraph: " + use.getInput() + ". Parse error at index " + use.getIndex());
            return toBeReturned;
        }
        log.debug("URI built for Blazegraph SPARQL endpoint: {}", uri);
        HttpResponse response;
        response = Request.Post(uri)
                .setHeader("Accept", SuggestServlet.IO_MIME_TYPE)
                .bodyForm(Form.form().add("update", query).build())
                .execute()
                .returnResponse();
        log.debug("Response from Blazegraph SPARQL endpoint: {}", response);
        int status = response.getStatusLine().getStatusCode();
        // Get the SPARQL update response only if it went wrong
        if (status == HttpServletResponse.SC_OK) {
            log.info("The state change via SPARQL update to Blazegraph went fine");
            return null;
        } else {
            log.error("Failed changing state via SPARQL update to Blazegraph. HTTP error code: {}", status);
            try (BufferedReader responseReader = new BufferedReader(new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8))) {
                JSONParser parser = new JSONParser();
                return (JSONObject) parser.parse(responseReader);
            } catch (ParseException pe) {
                log.error("Malformed JSON response from Blazegraph. Parse error at index {}", pe.getPosition());
                JSONObject toBeReturned = new JSONObject();
                toBeReturned.put("error_message", "Malformed JSON response from Blazegraph. Parse error at index: " + pe.getPosition());
                return toBeReturned;
            }
        }
    }

    private void sendResponse(HttpServletResponse response, JSONObject blazegraphResponse) throws IOException {
        response.setHeader("Access-Control-Allow-Origin", "*");
        if (blazegraphResponse == null) response.setStatus(HttpServletResponse.SC_OK);
        else {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.setContentType(SuggestServlet.IO_MIME_TYPE);
            response.setCharacterEncoding(StandardCharsets.UTF_8.name());
            try (PrintWriter pw = response.getWriter()) {
                blazegraphResponse.writeJSONString(pw);
            }
        }
    }

    private boolean parseQuickStatement(String quickStatement, RequestParameters parameters) {
        String[] elements = quickStatement.split("\t");
        if (elements.length < 3) {
            log.warn("Malformed QuickStatement: {}", quickStatement);
            return false;
        }
        String subject = elements[0];
        String mainProperty = elements[1];
        if (!VALIDATOR.isValidTerm(subject, "item")) {
            log.warn("Invalid subject QID: {}", subject);
            return false;
        }
        if (!VALIDATOR.isValidTerm(mainProperty, "property")) {
            log.warn("Invalid main property PID: {}", mainProperty);
            return false;
        }
        parameters.qId = subject;
        parameters.mainPId = mainProperty;
        List<String> qualifierOrReference = Arrays.asList(elements).subList(3, elements.length);
        switch (parameters.type) {
            case "claim":
                parameters.pId = mainProperty;
                parameters.value = quickStatementValueToRdf(elements[2]);
                break;
            case "qualifier":
                parameters.pId = qualifierOrReference.get(0);
                parameters.value = quickStatementValueToRdf(qualifierOrReference.get(1));
                break;
            case "reference":
                parameters.pId = qualifierOrReference.get(0).replace('S', 'P');
                parameters.value = quickStatementValueToRdf(qualifierOrReference.get(1));
                break;
        }
        return true;
    }

    private Value quickStatementValueToRdf(String qsValue) {
        ValueFactory vf = ValueFactoryImpl.getInstance();
        if (VALIDATOR.isValidTerm(qsValue, "item")) {
            org.openrdf.model.URI item = vf.createURI(SuggestServlet.WIKIBASE_URIS.entity(), qsValue);
            log.debug("Item value. From QuickStatement [{}] to RDF [{}]", qsValue, item);
            return item;
        } else if (qsValue.matches(MONOLINGUAL_TEXT.pattern())) {
            Matcher matcher = MONOLINGUAL_TEXT.matcher(qsValue);
            matcher.matches();
            Literal monolingual = vf.createLiteral(matcher.group(2).replace("\"", ""), matcher.group(1));
            log.debug("Monolingual text value. From QuickStatement [{}] to RDF [{}]", qsValue, monolingual);
            return monolingual;
        } else if (qsValue.matches(TIME.pattern())) {
            String[] elements = qsValue.split("/");
            WikibaseDate wbTime = WikibaseDate.fromString(elements[0]);
            Literal time = vf.createLiteral(wbTime.toString(WikibaseDate.ToStringFormat.DATE_TIME), XMLSchema.DATETIME);
            log.debug("Time value. From QuickStatement [{}] to RDF [{}]", qsValue, time);
            return time;
        } else if (qsValue.matches(LOCATION.pattern())) {
            Matcher matcher = LOCATION.matcher(qsValue);
            matcher.matches();
            String[] latLong = new String[2];
            latLong[0] = matcher.group(1);
            latLong[1] = matcher.group(2);
            WikibasePoint point = new WikibasePoint(latLong, SuggestServlet.DEFAULT_GLOBE);
            Literal location = vf.createLiteral(point.toString(), GeoSparql.WKT_LITERAL);
            log.debug("Location value. From QuickStatement [{}] to RDF [{}]", qsValue, location);
            return location;
        } else if (qsValue.matches(QUANTITY.pattern())) {
            Literal quantity = vf.createLiteral(qsValue, XMLSchema.DECIMAL);
            log.debug("Quantity value. From QuickStatement [{}] to RDF [{}]", qsValue, quantity);
            return quantity;
        } else {
            String noQuotes = qsValue.replace("\"", "");
            try {
                // URL
                org.openrdf.model.URI url = vf.createURI(noQuotes);
                log.debug("URL value. From QuickStatement [{}] to RDF [{}]", qsValue, url);
                return url;
            } catch (IllegalArgumentException iae) {
                // Plain string
                Literal plain = vf.createLiteral(noQuotes);
                log.debug("Plain string value. From QuickStatement [{}] to RDF [{}]", qsValue, plain);
                return plain;
            }
        }
    }

    private Value wikidataJsonReferenceValueToRdf(Object jsonValue) {
        ValueFactory vf = ValueFactoryImpl.getInstance();
        JSONParser p = new JSONParser();
        if (jsonValue instanceof JSONObject) {
            JSONObject actualValue = (JSONObject) jsonValue;
            JSONObject dataValue = (JSONObject) actualValue.get("datavalue");
            String dataValueType = (String) dataValue.get("type");
            JSONObject objectValue;
            String stringValue;
            switch (dataValueType) {
                case "wikibase-entityid":
                    try {
                        objectValue = (JSONObject) p.parse((String) dataValue.get("value"));
                    } catch (ParseException pe) {
                        log.error("Malformed reference JSON value. Parse error at index {}", pe.getPosition());
                        return null;
                    }
                    String id = Long.toString((long) objectValue.get("numeric-id"));
                    return vf.createURI(SuggestServlet.WIKIBASE_URIS.entity(), "Q" + id);
                case "string":
                    stringValue = (String) dataValue.get("value");
                    try {
                        // URL
                        return vf.createURI(stringValue);
                    } catch (IllegalArgumentException iae) {
                        // String
                        return vf.createLiteral(stringValue);
                    }
                case "monolingualtext":
                    try {
                        objectValue = (JSONObject) p.parse((String) dataValue.get("value"));
                    } catch (ParseException pe) {
                        log.error("Malformed reference JSON value. Parse error at index {}", pe.getPosition());
                        return null;
                    }
                    return vf.createLiteral((String) objectValue.get("text"), (String) objectValue.get("language"));
            }
        } else return null;
        return null;
    }

    private Value wikidataJsonValueToRdf(Object jsonValue) {
        ValueFactory vf = ValueFactoryImpl.getInstance();
        JSONParser parser = new JSONParser();
        // Values that are JSON objects are passed to the MW API as strings, cool!
        JSONObject jsonObjectValue;
        if (jsonValue instanceof String) {
            String stringValue = (String) jsonValue;
            try {
                jsonObjectValue = (JSONObject) parser.parse(stringValue);
            } catch (ParseException pe) {
                // Yes, values explicitly typed as strings may also be URLs, nice!
                try {
                    // URL
                    return vf.createURI(stringValue);
                } catch (IllegalArgumentException iae) {
                    // String
                    return vf.createLiteral(stringValue);
                }
            }
        } else return null;
        // Item
        if (jsonObjectValue.containsValue("item")) {
            String id = Long.toString((long) jsonObjectValue.get("numeric-id"));
            return vf.createURI(SuggestServlet.WIKIBASE_URIS.entity(), "Q" + id);
        }
        // Monolingual text
        else if (jsonObjectValue.containsKey("language")) {
            return vf.createLiteral((String) jsonObjectValue.get("text"), (String) jsonObjectValue.get("language"));
        }
        // Globe coordinate
        else if (jsonObjectValue.containsKey("globe")) {
            String[] latLong = new String[2];
            latLong[0] = Double.toString((double) jsonObjectValue.get("latitude"));
            latLong[1] = Double.toString((double) jsonObjectValue.get("longitude"));
            WikibasePoint point = new WikibasePoint(latLong, (String) jsonObjectValue.get("globe"));
            return vf.createLiteral(point.toString(), GeoSparql.WKT_LITERAL);
        }
        // Time
        else if (jsonObjectValue.containsKey("time")) {
            WikibaseDate date = WikibaseDate.fromString((String) jsonObjectValue.get("time"));
            return vf.createLiteral(date.toString(WikibaseDate.ToStringFormat.DATE_TIME), XMLSchema.DATETIME);
        }
        // Quantity
        else if (jsonObjectValue.containsKey("amount")) {
            return vf.createLiteral((String) jsonObjectValue.get("amount"), XMLSchema.DECIMAL);
        }
        return null;
    }
}
