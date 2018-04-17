package org.wikidata.query.rdf.primarysources.statistics;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.openrdf.model.Value;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
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
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.Enumeration;

/**
 * @author Marco Fossati - User:Hjfocs
 * @since 0.2.5
 * Created on Dec 20, 2017.
 */
public class StatisticsServlet extends HttpServlet {

    private static final Logger log = LoggerFactory.getLogger(StatisticsServlet.class);

    private class RequestParameters {
        public String dataset;
        public String user;
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        RequestParameters parameters = new RequestParameters();
        boolean ok = processRequest(request, parameters, response);
        if (!ok) return;
        log.debug("Required parameters stored as fields in private class: {}", parameters);
        if (parameters.dataset != null) {
            JSONObject datasetStatistics = getDatasetStatistics(parameters.dataset);
            if (datasetStatistics != null) log.info("Loaded datasets statistics from cache");
            sendResponse(response, datasetStatistics, ApiParameters.DATASET_PARAMETER, parameters);
            log.info("GET /statistics for datasets successful");
            return;
        }
        if (parameters.user != null) {
            sendResponse(response, getUserStatistics(parameters.user), ApiParameters.USER_NAME_PARAMETER, parameters);
            log.info("GET /statistics for users successful");
        }
    }

    private boolean processRequest(HttpServletRequest request, RequestParameters parameters, HttpServletResponse response) throws IOException {
        Enumeration<String> params = request.getParameterNames();
        String datasetOrUser = params.nextElement();
        if (params.hasMoreElements()) {
            log.warn("More than one parameter given, will fail with a bad request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Only one parameter is required, either 'dataset' or 'user");
            return false;
        }
        String datasetOrUserValue = request.getParameter(datasetOrUser);
        if (datasetOrUserValue.isEmpty()) {
            log.warn("Empty {} value. Will fail with a bad request", datasetOrUser);
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "The parameter '" + datasetOrUser + "' cannot have an empty value.");
            return false;
        }
        switch (datasetOrUser) {
            case ApiParameters.DATASET_PARAMETER:
                try {
                    new URI(datasetOrUserValue);
                } catch (URISyntaxException use) {
                    log.warn("Invalid dataset URI: {}. Parse error at index {}. Will fail with a bad request", use.getInput(), use.getIndex());
                    response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid dataset URI: <" + use.getInput() + ">. " +
                            "Parse error at index " + use.getIndex() + ".");
                    return false;
                }
                parameters.dataset = datasetOrUserValue;
                parameters.user = null;
                return true;
            case ApiParameters.USER_NAME_PARAMETER:
                boolean validated = Utils.validateUserName(datasetOrUserValue);
                if (!validated) {
                    log.warn("Invalid user name. Will fail with a bad request");
                    response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Illegal characters found in the user name: '" + datasetOrUserValue + "'. The following characters are not allowed: : / ? # [ ] @ ! $ & ' ( ) * + , ; =");
                    return false;
                }
                parameters.user = datasetOrUserValue;
                parameters.dataset = null;
                return true;
            default:
                log.warn("Invalid required parameter: {}. Will fail with a bad request", datasetOrUser);
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid required parameter: '" + datasetOrUser + "'. Use one of 'dataset' or 'user");
                return false;
        }
    }

    private void sendResponse(HttpServletResponse response, JSONObject output, String datasetOrUser, RequestParameters parameters) throws IOException {
        response.setHeader("Access-Control-Allow-Origin", "*");
        if (output == null) {
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Something went wrong when retrieving " + datasetOrUser + " statistics.");
        } else if (output.isEmpty()) {
            String errorMessage = datasetOrUser.equals("dataset") ? "No statistics available for dataset <" + parameters.dataset + "> ." : "No activity for user '" + parameters.user + "'.";
            log.warn(errorMessage + " Will fail with a not found");
            response.sendError(HttpServletResponse.SC_NOT_FOUND, errorMessage);
        } else {
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType(ApiParameters.DEFAULT_IO_MIME_TYPE);
            try (PrintWriter pw = response.getWriter()) {
                output.writeJSONString(pw);
            }
        }
    }

    private JSONObject getDatasetStatistics(String dataset) throws IOException {
        JSONParser parser = new JSONParser();
        Object parsed;
        try (BufferedReader reader = Files.newBufferedReader(Config.DATASETS_CACHE_PATH)) {
            try {
                parsed = parser.parse(reader);
            } catch (ParseException pe) {
                log.error("Malformed JSON datasets statistics. Parse error at index {}. Please check {}", pe.getPosition(), Config.DATASETS_CACHE_PATH);
                return null;
            }
        }
        JSONObject allStats = (JSONObject) parsed;
        Object datasetStats = allStats.get(dataset);
        if (datasetStats == null) return new JSONObject();
        else {
            JSONObject stats = (JSONObject) datasetStats;
            log.debug("Dataset statistics from cache file '{}:' {}", Config.DATASETS_CACHE_PATH, stats);
            // Get dataset description and uploader user name via SPARQL
            String query = SparqlQueries.DATASET_INFO_QUERY.replace(SparqlQueries.DATASET_PLACE_HOLDER, dataset);
            TupleQueryResult result = Utils.runSparqlQuery(query);
            if (result == null) return null;
            try {
                while (result.hasNext()) {
                    Value descriptionOrUploader = result.next().getValue("description_or_uploader");
                    if (descriptionOrUploader instanceof org.openrdf.model.URI)
                        stats.put("uploader", descriptionOrUploader.stringValue());
                    else stats.put("description", descriptionOrUploader.stringValue());
                }
            } catch (QueryEvaluationException qee) {
                log.error("Failed evaluating the dataset description and uploader query: '" + query + "' The stack trace follows.", qee);
                return null;
            }
            log.debug("Final dataset statistics with uploader user name and eventual dataset description: {}", stats);
            return stats;
        }
    }

    private JSONObject getUserStatistics(String user) {
        JSONObject stats = new JSONObject();
        String query = SparqlQueries.USER_INFO_QUERY.replace(SparqlQueries.USER_PLACE_HOLDER, user);
        TupleQueryResult result = Utils.runSparqlQuery(query);
        if (result == null) return null;
        try {
            if (result.hasNext()) {
                int activities = Integer.parseInt(result.next().getValue("activities").stringValue());
                if (result.hasNext()) log.warn("The user statistics query yielded more than one result. " +
                        "This should not happen, please check the database. " +
                        "Only the first result will be used. " +
                        "Query: {}. Result: {}", query, result);
                stats.put("user", user);
                stats.put("activities", activities);
            }
        } catch (QueryEvaluationException qee) {
            log.error("Failed evaluating the user statistics query: '" + query + "' The stack trace follows.", qee);
            return null;
        }
        return stats;
    }
}
