package org.wikidata.query.rdf.primarysources.curation;

import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Form;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.utils.URIBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.WikibaseDate;
import org.wikidata.query.rdf.common.WikibasePoint;
import org.wikidata.query.rdf.common.uri.GeoSparql;
import org.wikidata.query.rdf.primarysources.WikibaseDataModelValidator;

import javax.servlet.ServletException;
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

import static org.wikidata.query.rdf.primarysources.ingestion.UploadServlet.*;

/**
 * @author Marco Fossati - User:Hjfocs
 * @since 0.2.4
 * Created on Aug 30, 2017.
 */
public class CurateServlet extends HttpServlet {

    private static final String MW_API_OBJECT = "for_mw_api";
    private static final String PID_KEY = "property";
    private static final String MAIN_PID_KEY = "main_property";
    private static final String VALUE_KEY = "value";
    private static final String STATEMENT_TYPE_KEY = "type";
    private static final String STATE_KEY = "state";
    private static final String USER_KEY = "user";

    private static final String MAIN_PID_PLACE_HOLDER = "${MAIN_PID}";
    private static final String PID_PLACE_HOLDER = "${PID}";
    private static final String VALUE_PLACE_HOLDER = "${VALUE}";
    private static final String STATE_PLACE_HOLDER = "${STATE}";

    // Approve claim + eventual qualifiers
    private static final String CLAIM_APPROVAL_QUERY =
            "DELETE {" +
                    "  GRAPH <" + SuggestServlet.DATASET_PLACE_HOLDER + "/new> {" +
                    "    ?claim ps:" + PID_PLACE_HOLDER + " " + VALUE_PLACE_HOLDER + " ;" +
                    "           ?pq ?qualifier ." +
                    "  }" +
                    "}" +
                    "INSERT {" +
                    "  GRAPH <" + SuggestServlet.DATASET_PLACE_HOLDER + "/approved> {" +
                    "    ?claim ps:" + PID_PLACE_HOLDER + " " + VALUE_PLACE_HOLDER + " ;" +
                    "           ?pq ?qualifier ." +
                    "  }" +
                    "}" +
                    "WHERE {" +
                    "  ?claim ps:" + PID_PLACE_HOLDER + " " + VALUE_PLACE_HOLDER + " ." +
                    "  OPTIONAL {" +
                    "    ?claim ?pq ?qualifier ." +
                    "    FILTER (?pq != prov:wasDerivedFrom) ." +
                    "  }" +
                    "}";
    // Reject everything
    private static final String CLAIM_REJECTION_QUERY =
            "DELETE {" +
                    "  GRAPH <" + SuggestServlet.DATASET_PLACE_HOLDER + "/new> {" +
                    "    wd:" + SuggestServlet.QID_PLACE_HOLDER + " p:" + MAIN_PID_PLACE_HOLDER + " ?st_node ." +
                    "    ?st_node ps:" + PID_PLACE_HOLDER + " " + VALUE_PLACE_HOLDER + " ;" +
                    "             prov:wasDerivedFrom ?ref_node ;" +
                    "             ?qualif_p ?qualif_v ." +
                    "    ?ref_node ?ref_p ?ref_v" +
                    "  }" +
                    "}" +
                    "INSERT {" +
                    "  GRAPH <" + SuggestServlet.DATASET_PLACE_HOLDER + "/rejected> {" +
                    "    wd:" + SuggestServlet.QID_PLACE_HOLDER + " p:" + MAIN_PID_PLACE_HOLDER + " ?st_node ." +
                    "    ?st_node ps:" + PID_PLACE_HOLDER + " " + VALUE_PLACE_HOLDER + " ;" +
                    "             prov:wasDerivedFrom ?ref_node ;" +
                    "             ?qualif_p ?qualif_v ." +
                    "    ?ref_node ?ref_p ?ref_v" +
                    "  }" +
                    "}" +
                    "WHERE {" +
                    "  wd:" + SuggestServlet.QID_PLACE_HOLDER + " p:" + MAIN_PID_PLACE_HOLDER + " ?st_node ." +
                    "  ?st_node ps:" + PID_PLACE_HOLDER + " " + VALUE_PLACE_HOLDER + " ;" +
                    "  OPTIONAL {" +
                    "    ?st_node prov:wasDerivedFrom ?ref_node ." +
                    "    ?ref_node ?ref_p ?ref_v ." +
                    "  }" +
                    "  OPTIONAL {" +
                    "    ?st_node ?qualif_p ?qualif_v ." +
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
                    "}" +
                    "INSERT {" +
                    "  GRAPH <" + SuggestServlet.DATASET_PLACE_HOLDER + "/" + STATE_PLACE_HOLDER + "> {" +
                    "    ?st_node ps:" + MAIN_PID_PLACE_HOLDER + " ?st_value ;" +
                    "             prov:wasDerivedFrom ?ref_node ." +
                    "    ?qualifier pq:" + PID_PLACE_HOLDER + " " + VALUE_PLACE_HOLDER + " ." +
                    "  }" +
                    "}" +
                    "WHERE {" +
                    "  wd:" + SuggestServlet.QID_PLACE_HOLDER + " p:" + MAIN_PID_PLACE_HOLDER + " ?st_node ." +
                    "  ?st_node ps:" + MAIN_PID_PLACE_HOLDER + " ?st_value ." +
                    "  ?qualifier pq:" + PID_PLACE_HOLDER + " " + VALUE_PLACE_HOLDER + " ." +
                    "  OPTIONAL {" +
                    "    ?st_node prov:wasDerivedFrom ?ref_node ." +
                    "    ?ref_node ?ref_p ?ref_v ." +
                    "  }" +
                    "}";

    // Approve/reject everything but main node + sibling references
    private static final String REFERENCE_QUERY =
            "DELETE {" +
                    "  GRAPH <" + SuggestServlet.DATASET_PLACE_HOLDER + "/new> {" +
                    "    ?st_node ps:" + MAIN_PID_PLACE_HOLDER + " ?st_value ;" +
                    "             prov:wasDerivedFrom ?ref_node ;" +
                    "             ?qualif_p ?qualif_v ." +
                    "    ?ref_node pr:" + PID_PLACE_HOLDER + " " + VALUE_PLACE_HOLDER + " ." +
                    "  }" +
                    "}" +
                    "INSERT {" +
                    "  GRAPH <" + SuggestServlet.DATASET_PLACE_HOLDER + "/" + STATE_PLACE_HOLDER + "> {" +
                    "    ?st_node ps:" + MAIN_PID_PLACE_HOLDER + " ?st_value ;" +
                    "             prov:wasDerivedFrom ?ref_node ;" +
                    "             ?qualif_p ?qualif_v ." +
                    "    ?ref_node pr:" + PID_PLACE_HOLDER + " " + VALUE_PLACE_HOLDER + " ." +
                    "  }" +
                    "}" +
                    "WHERE {" +
                    "  wd:" + SuggestServlet.QID_PLACE_HOLDER + " p:" + MAIN_PID_PLACE_HOLDER + " ?st_node ." +
                    "  ?st_node ps:" + MAIN_PID_PLACE_HOLDER + " ?st_value ." +
                    "  ?ref_node pr:" + PID_PLACE_HOLDER + " " + VALUE_PLACE_HOLDER + " ." +
                    "  OPTIONAL {" +
                    "    ?st_node ?qualif_p ?qualif_v ." +
                    "    FILTER (?qualif_p != prov:wasDerivedFrom) ." +
                    "  }" +
                    "}";
    private static final Logger log = LoggerFactory.getLogger(CurateServlet.class);

    private String qId;
    private String pId;
    private String mainPId;
    private Value value;
    private String type;
    private String state;
    // todo use this field for https://phabricator.wikimedia.org/T170820
    private String user;
    private String dataset;

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        boolean ok = processRequest(request, response);
        if (!ok) return;
        JSONObject blazegraphError = changeState();
        sendResponse(response, blazegraphError);
    }

    private boolean processRequest(HttpServletRequest request, HttpServletResponse response) throws IOException {
        WikibaseDataModelValidator validator = new WikibaseDataModelValidator();
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
        } else if (!validator.isValidTerm(givenQId, "item")) {
            log.error("Invalid QID: {}. Will fail with a bad request.", givenQId);
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid QID: '" + givenQId + "'");
            return false;
        }
        qId = givenQId;
        String givenMainPId = (String) body.get(MAIN_PID_KEY);
        if (givenMainPId == null) {
            log.error("No main PID given. will fail with a bad request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing required main PID.");
            return false;
        } else if (!validator.isValidTerm(givenMainPId, "property")) {
            log.error("Invalid main PID: {}. Will fail with a bad request.", givenMainPId);
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid main PID: '" + givenMainPId + "'");
            return false;
        }
        mainPId = givenMainPId;
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
            } else if (!validator.isValidTerm(givenPId, "property")) {
                log.error("Invalid PID: {}. Will fail with a bad request.", givenPId);
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid PID: '" + givenPId + "'");
                return false;
            }
            pId = givenPId;
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
            } else if (!validator.isValidTerm(givenPId, "property")) {
                log.error("Invalid PID: {}. Will fail with a bad request.", givenPId);
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid PID: '" + givenPId + "'");
                return false;
            }
            pId = givenPId;
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
        value = rdfValue;
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
        type = givenType;
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
        state = givenState;
        String givenUser = (String) body.get(USER_KEY);
        if (givenUser == null) {
            log.error("No user name given. Will fail with a bad request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing required user name.");
            return false;
        }
        user = givenUser;
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
        dataset = givenDataset.replace("/new", "");
        return true;
    }

    private JSONObject changeState() throws IOException {
        String query = null;
        switch (type) {
            case "claim":
                query = state.equals("approved") ? CLAIM_APPROVAL_QUERY : CLAIM_REJECTION_QUERY;
                query = query
                        .replace(SuggestServlet.DATASET_PLACE_HOLDER, dataset)
                        .replace(STATE_PLACE_HOLDER, state)
                        .replace(SuggestServlet.QID_PLACE_HOLDER, qId)
                        .replace(MAIN_PID_PLACE_HOLDER, mainPId)
                        .replace(PID_PLACE_HOLDER, pId);
                query = value instanceof org.openrdf.model.URI
                        ? query.replace(VALUE_PLACE_HOLDER, "<" + value.toString() + ">")
                        : query.replace(VALUE_PLACE_HOLDER, value.toString());
                break;
            case "qualifier":
                query = QUALIFIER_QUERY
                        .replace(SuggestServlet.DATASET_PLACE_HOLDER, dataset)
                        .replace(STATE_PLACE_HOLDER, state)
                        .replace(SuggestServlet.QID_PLACE_HOLDER, qId)
                        .replace(MAIN_PID_PLACE_HOLDER, mainPId)
                        .replace(PID_PLACE_HOLDER, pId);
                query = value instanceof org.openrdf.model.URI
                        ? query.replace(VALUE_PLACE_HOLDER, "<" + value.toString() + ">")
                        : query.replace(VALUE_PLACE_HOLDER, value.toString());
                break;
            case "reference":
                query = REFERENCE_QUERY
                        .replace(SuggestServlet.DATASET_PLACE_HOLDER, dataset)
                        .replace(STATE_PLACE_HOLDER, state)
                        .replace(SuggestServlet.QID_PLACE_HOLDER, qId)
                        .replace(MAIN_PID_PLACE_HOLDER, mainPId)
                        .replace(PID_PLACE_HOLDER, pId);
                query = value instanceof org.openrdf.model.URI
                        ? query.replace(VALUE_PLACE_HOLDER, "<" + value.toString() + ">")
                        : query.replace(VALUE_PLACE_HOLDER, value.toString());
                break;
        }
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
        HttpResponse response;
        response = Request.Post(uri)
                .setHeader("Accept", SuggestServlet.IO_MIME_TYPE)
                .bodyForm(Form.form().add("update", query).build())
                .execute()
                .returnResponse();
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
