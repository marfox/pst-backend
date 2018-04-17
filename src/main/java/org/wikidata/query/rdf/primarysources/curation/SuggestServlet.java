package org.wikidata.query.rdf.primarysources.curation;

import org.json.simple.JSONArray;
import org.openrdf.query.TupleQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.primarysources.common.ApiParameters;
import org.wikidata.query.rdf.primarysources.common.Utils;
import org.wikidata.query.rdf.primarysources.common.WikibaseDataModelValidator;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author Marco Fossati - User:Hjfocs
 * @since 0.2.4
 * Created on Aug 04, 2017.
 */
public class SuggestServlet extends HttpServlet {

    private static final Logger log = LoggerFactory.getLogger(SuggestServlet.class);

    private class RequestParameters {
        public String dataset;
        public String qId;
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        RequestParameters parameters = new RequestParameters();
        boolean ok = processRequest(request, parameters, response);
        if (!ok) return;
        log.debug("Required parameters stored as fields in private class: {}", parameters);
        TupleQueryResult suggestions = Utils.getSuggestions(parameters.dataset, parameters.qId);
        sendResponse(response, parameters, suggestions);
        log.info("GET /suggest successful");
    }

    private boolean processRequest(HttpServletRequest request, RequestParameters parameters, HttpServletResponse response) throws IOException {
        WikibaseDataModelValidator validator = new WikibaseDataModelValidator();
        parameters.qId = request.getParameter(ApiParameters.QID_PARAMETER);
        if (parameters.qId == null) {
            log.warn("No QID given. Will fail with a bad request.");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing required QID.");
            return false;
        } else if (!validator.isValidTerm(parameters.qId, "item")) {
            log.warn("Invalid QID: {}. Will fail with a bad request.", parameters.qId);
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid QID: '" + parameters.qId + "'");
            return false;
        }
        String datasetParameter = request.getParameter(ApiParameters.DATASET_PARAMETER);
        if (datasetParameter == null || datasetParameter.isEmpty()) {
            parameters.dataset = "all";
        } else {
            try {
                new URI(datasetParameter);
                parameters.dataset = datasetParameter;
            } catch (URISyntaxException use) {
                log.warn("Invalid dataset URI: {}. Parse error at index {}. Will fail with a bad request", use.getInput(), use.getIndex());
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid dataset URI: <" + use.getInput() + ">. " +
                        "Parse error at index " + use.getIndex() + ".");
                return false;
            }
        }
        return true;
    }

    private void sendResponse(HttpServletResponse response, RequestParameters parameters, TupleQueryResult suggestions) throws IOException {
        response.setHeader("Access-Control-Allow-Origin", "*");
        JSONArray jsonSuggestions = Utils.formatSuggestions(suggestions, parameters.dataset, parameters.qId);
        if (jsonSuggestions == null) {
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Something went wrong when retrieving suggestions.");
        } else if (jsonSuggestions.isEmpty()) {
            log.warn("No suggestions available for item {}. Will fail with a not found", parameters.qId);
            response.sendError(HttpServletResponse.SC_NOT_FOUND, "No suggestions available for item " + parameters.qId + " .");
        } else {
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType(ApiParameters.DEFAULT_IO_MIME_TYPE);
            try (PrintWriter pw = response.getWriter()) {
                jsonSuggestions.writeJSONString(pw);
            }
        }
    }

}
