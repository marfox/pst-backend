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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.primarysources.common.ApiParameters;
import org.wikidata.query.rdf.primarysources.common.Config;
import org.wikidata.query.rdf.primarysources.common.SparqlQueries;
import org.wikidata.query.rdf.primarysources.common.Utils;

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

    private static final Logger log = LoggerFactory.getLogger(CurateServlet.class);

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
        String givenState = (String) body.get(ApiParameters.STATEMENT_STATE_JSON_KEY);
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
        String givenUser = (String) body.get(ApiParameters.USER_NAME_PARAMETER);
        if (givenUser == null) {
            log.warn("No user name given. Will fail with a bad request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing required user name.");
            return false;
        }
        boolean validated = Utils.validateUserName(givenUser);
        if (!validated) {
            log.warn("Invalid user name. Will fail with a bad request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Illegal characters found in the user name: '" + givenUser + "'. The following characters are not allowed: : / ? # [ ] @ ! $ & ' ( ) * + , ; =");
            return false;
        }
        parameters.user = givenUser;
        String givenDataset = (String) body.get(ApiParameters.DATASET_PARAMETER);
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
        String givenType = (String) body.get(ApiParameters.STATEMENT_TYPE_JSON_KEY);
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
        String givenQuickStatement = (String) body.get(ApiParameters.QUICKSTATEMENT_JSON_KEY);
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
        String givenQId = (String) body.get(ApiParameters.QID_PARAMETER);
        if (givenQId == null) {
            log.error("No QID given. Will fail with a bad request.");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing required QID.");
            return false;
        } else if (!Utils.VALIDATOR.isValidTerm(givenQId, "item")) {
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
        } else if (!Utils.VALIDATOR.isValidTerm(givenMainPId, "property")) {
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
            } else if (!Utils.VALIDATOR.isValidTerm(givenPId, "property")) {
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
            rdfValue = Utils.wikidataJsonReferenceValueToRdf(jsonValue);
        } else {
            String givenPId = (String) mwApiObject.get(PID_KEY);
            if (givenPId == null) {
                log.error("No PID given. will fail with a bad request");
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing required PID.");
                return false;
            } else if (!Utils.VALIDATOR.isValidTerm(givenPId, "property")) {
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
            rdfValue = Utils.wikidataJsonValueToRdf(jsonValue);
        }
        if (rdfValue == null) {
            log.error("Unexpected JSON value: {}, Will fail with a bad request", jsonValue);
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Unexpected JSON value: '" + jsonValue + "'. Expected an object or a string");
            return false;
        }
        parameters.value = rdfValue;
        String givenType = (String) body.get(ApiParameters.STATEMENT_TYPE_JSON_KEY);
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
        String givenState = (String) body.get(ApiParameters.STATEMENT_STATE_JSON_KEY);
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
        String givenUser = (String) body.get(ApiParameters.USER_NAME_PARAMETER);
        if (givenUser == null) {
            log.error("No user name given. Will fail with a bad request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing required user name.");
            return false;
        }
        boolean validated = Utils.validateUserName(givenUser);
        if (!validated) {
            log.warn("Invalid user name. Will fail with a bad request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Illegal characters found in the user name: '" + givenUser + "'. The following characters are not allowed: : / ? # [ ] @ ! $ & ' ( ) * + , ; =");
            return false;
        }
        parameters.user = givenUser;
        String givenDataset = (String) body.get(ApiParameters.DATASET_PARAMETER);
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
            query = parameters.state.equals("approved") ? SparqlQueries.CLAIM_APPROVAL_QUERY : SparqlQueries.CLAIM_REJECTION_QUERY;
            query = query
                .replace(SparqlQueries.USER_PLACE_HOLDER, parameters.user)
                .replace(SparqlQueries.DATASET_PLACE_HOLDER, parameters.dataset)
                .replace(SparqlQueries.STATE_PLACE_HOLDER, parameters.state)
                .replace(SparqlQueries.QID_PLACE_HOLDER, parameters.qId)
                .replace(SparqlQueries.MAIN_PID_PLACE_HOLDER, parameters.mainPId)
                .replace(SparqlQueries.PID_PLACE_HOLDER, parameters.pId);
            query = parameters.value instanceof org.openrdf.model.URI
                ? query.replace(SparqlQueries.ITEM_VALUE_PLACE_HOLDER, "<" + parameters.value.toString() + ">")
                : query.replace(SparqlQueries.ITEM_VALUE_PLACE_HOLDER, parameters.value.toString());
            break;
        case "qualifier":
            query = SparqlQueries.QUALIFIER_CURATION_QUERY
                .replace(SparqlQueries.USER_PLACE_HOLDER, parameters.user)
                .replace(SparqlQueries.DATASET_PLACE_HOLDER, parameters.dataset)
                .replace(SparqlQueries.STATE_PLACE_HOLDER, parameters.state)
                .replace(SparqlQueries.QID_PLACE_HOLDER, parameters.qId)
                .replace(SparqlQueries.MAIN_PID_PLACE_HOLDER, parameters.mainPId)
                .replace(SparqlQueries.PID_PLACE_HOLDER, parameters.pId);
            query = parameters.value instanceof org.openrdf.model.URI
                ? query.replace(SparqlQueries.ITEM_VALUE_PLACE_HOLDER, "<" + parameters.value.toString() + ">")
                : query.replace(SparqlQueries.ITEM_VALUE_PLACE_HOLDER, parameters.value.toString());
            break;
        case "reference":
            query = SparqlQueries.REFERENCE_CURATION_QUERY
                .replace(SparqlQueries.USER_PLACE_HOLDER, parameters.user)
                .replace(SparqlQueries.DATASET_PLACE_HOLDER, parameters.dataset)
                .replace(SparqlQueries.STATE_PLACE_HOLDER, parameters.state)
                .replace(SparqlQueries.QID_PLACE_HOLDER, parameters.qId)
                .replace(SparqlQueries.MAIN_PID_PLACE_HOLDER, parameters.mainPId)
                .replace(SparqlQueries.PID_PLACE_HOLDER, parameters.pId);
            query = parameters.value instanceof org.openrdf.model.URI
                ? query.replace(SparqlQueries.ITEM_VALUE_PLACE_HOLDER, "<" + parameters.value.toString() + ">")
                : query.replace(SparqlQueries.ITEM_VALUE_PLACE_HOLDER, parameters.value.toString());
            break;
        }
        log.debug("SPARQL update query to be sent to Blazegraph: {}", query);
        URIBuilder builder = new URIBuilder();
        URI uri;
        try {
            uri = builder
                .setScheme("http")
                .setHost(Config.BLAZEGRAPH_HOST)
                .setPort(Config.BLAZEGRAPH_PORT)
                .setPath(Config.BLAZEGRAPH_CONTEXT + Config.BLAZEGRAPH_SPARQL_ENDPOINT)
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
            .setHeader("Accept", ApiParameters.DEFAULT_IO_MIME_TYPE)
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
        if (blazegraphResponse == null) response.setStatus(HttpServletResponse.SC_OK);
        else {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.setContentType(ApiParameters.DEFAULT_IO_MIME_TYPE);
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
        if (!Utils.VALIDATOR.isValidTerm(subject, "item")) {
            log.warn("Invalid subject QID: {}", subject);
            return false;
        }
        if (!Utils.VALIDATOR.isValidTerm(mainProperty, "property")) {
            log.warn("Invalid main property PID: {}", mainProperty);
            return false;
        }
        parameters.qId = subject;
        parameters.mainPId = mainProperty;
        List<String> qualifierOrReference = Arrays.asList(elements).subList(3, elements.length);
        switch (parameters.type) {
        case "claim":
            parameters.pId = mainProperty;
            parameters.value = Utils.quickStatementValueToRdf(elements[2]);
            break;
        case "qualifier":
            parameters.pId = qualifierOrReference.get(0);
            parameters.value = Utils.quickStatementValueToRdf(qualifierOrReference.get(1));
            break;
        case "reference":
            parameters.pId = qualifierOrReference.get(0).replace('S', 'P');
            parameters.value = Utils.quickStatementValueToRdf(qualifierOrReference.get(1));
            break;
        }
        return true;
    }

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

}
