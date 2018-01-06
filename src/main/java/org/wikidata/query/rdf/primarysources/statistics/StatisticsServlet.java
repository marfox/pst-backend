package org.wikidata.query.rdf.primarysources.statistics;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

import static org.wikidata.query.rdf.primarysources.common.DatasetsStatisticsCache.DATASETS_CACHE_PATH;
import static org.wikidata.query.rdf.primarysources.curation.SuggestServlet.DATASET_PARAMETER;
import static org.wikidata.query.rdf.primarysources.curation.SuggestServlet.IO_MIME_TYPE;

/**
 * @author Marco Fossati - User:Hjfocs
 * @since 0.2.5
 * Created on Dec 20, 2017.
 */
public class StatisticsServlet extends HttpServlet {

    private static final String USER_PARAMETER = "user";
    private static final Logger log = LoggerFactory.getLogger(StatisticsServlet.class);

    private String dataset;
    private String user;

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        boolean ok = processRequest(request, response);
        if (!ok) return;
        if (dataset != null) {
            sendResponse(response, getCachedStatistics(dataset), DATASET_PARAMETER);
            return;
        }
        if (user != null) {
            // TODO user statistics https://phabricator.wikimedia.org/T183370
            // sendResponse(response, getCachedStatistics(user), USER_PARAMETER);
        }
    }

    private boolean processRequest(HttpServletRequest request, HttpServletResponse response) throws IOException {
        Enumeration<String> params = request.getParameterNames();
        String datasetOrUser = params.nextElement();
        if (params.hasMoreElements()) {
            log.error("More than one parameters given, will fail with a bad request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Only one parameter is required, either 'dataset' or 'user");
            return false;
        }
        String datasetOrUserValue = request.getParameter(datasetOrUser);
        if (datasetOrUserValue.isEmpty()) {
            log.error("Empty {} value. Will fail with a bad request", datasetOrUser);
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "The parameter '" + datasetOrUser + "' cannot have an empty value.");
        }
        switch (datasetOrUser) {
            case DATASET_PARAMETER:
                try {
                    new URI(datasetOrUserValue);
                } catch (URISyntaxException use) {
                    log.error("Invalid dataset URI: {}. Parse error at index {}. Will fail with a bad request", use.getInput(), use.getIndex());
                    response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid dataset URI: <" + use.getInput() + ">. " +
                            "Parse error at index " + use.getIndex() + ".");
                    return false;
                }
                dataset = datasetOrUserValue;
                return true;
            case USER_PARAMETER:
                // TODO handle user output
                user = datasetOrUserValue;
                return true;
            default:
                log.error("Invalid required parameter: {}. Will fail with a bad request", datasetOrUser);
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid required parameter: '" + datasetOrUser + "'. Use one of 'dataset' or 'user");
        }
        return true;
    }

    private void sendResponse(HttpServletResponse response, JSONObject output, String datasetOrUser) throws IOException {
        response.setHeader("Access-Control-Allow-Origin", "*");
        if (output == null) {
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Something went wrong when retrieving " + datasetOrUser + " statistics.");
        } else if (output.isEmpty()) {
            String errorMessage = datasetOrUser.equals("dataset") ? "No statistics available for dataset <" + dataset + "> ." : "No statistics available for user <" + user + "> .";
            response.sendError(HttpServletResponse.SC_NOT_FOUND, errorMessage);
        } else {
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType(IO_MIME_TYPE);
            try (PrintWriter pw = response.getWriter()) {
                output.writeJSONString(pw);
            }
        }
    }

    private JSONObject getCachedStatistics(String datasetOrUser) throws IOException {
        JSONParser parser = new JSONParser();
        Object parsed;
        try (BufferedReader reader = Files.newBufferedReader(DATASETS_CACHE_PATH)) {
            try {
                parsed = parser.parse(reader);
            } catch (ParseException pe) {
                log.error("Malformed JSON datasets statistics. Parse error at index {}. Please check {}", pe.getPosition(), DATASETS_CACHE_PATH);
                return null;
            }
        }
        JSONObject allStats = (JSONObject) parsed;
        return (JSONObject) allStats.getOrDefault(datasetOrUser, new JSONObject());
    }
}
