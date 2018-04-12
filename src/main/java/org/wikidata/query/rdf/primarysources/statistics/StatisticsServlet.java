package org.wikidata.query.rdf.primarysources.statistics;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.openrdf.model.Value;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.wikidata.query.rdf.primarysources.common.DatasetsStatisticsCache.DATASETS_CACHE_PATH;
import static org.wikidata.query.rdf.primarysources.curation.CurateServlet.USER_PLACE_HOLDER;
import static org.wikidata.query.rdf.primarysources.curation.SuggestServlet.*;
import static org.wikidata.query.rdf.primarysources.ingestion.UploadServlet.METADATA_NAMESPACE;
import static org.wikidata.query.rdf.primarysources.ingestion.UploadServlet.USER_URI_PREFIX;

/**
 * @author Marco Fossati - User:Hjfocs
 * @since 0.2.5
 * Created on Dec 20, 2017.
 */
public class StatisticsServlet extends HttpServlet {

    private static final String USER_QUERY =
            "SELECT ?activities " +
                    "WHERE {" +
                    "  GRAPH <" + METADATA_NAMESPACE + "> {" +
                    "    <" + USER_URI_PREFIX + USER_PLACE_HOLDER + "> <" + METADATA_NAMESPACE + "/activities> ?activities ." +
                    "  }" +
                    "}";
    private static final String DATASET_INFO_QUERY =
            "SELECT ?description_or_uploader " +
                    "WHERE {" +
                    "  GRAPH <" + METADATA_NAMESPACE + "> {" +
                    "    <" + DATASET_PLACE_HOLDER + "> ?p ?description_or_uploader ." +
                    "  }" +
                    "}";
    private static final String USER_PARAMETER = "user";
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
        if (parameters.dataset != null) {
            sendResponse(response, getDatasetStatistics(parameters.dataset), DATASET_PARAMETER, parameters);
            return;
        }
        if (parameters.user != null) {
            sendResponse(response, getUserStatistics(parameters.user), USER_PARAMETER, parameters);
        }
    }

    private boolean processRequest(HttpServletRequest request, RequestParameters parameters, HttpServletResponse response) throws IOException {
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
                parameters.dataset = datasetOrUserValue;
                parameters.user = null;
                return true;
            case USER_PARAMETER:
                // URI reserved characters are not allowed https://tools.ietf.org/html/rfc3986#section-2.2
                Pattern illegal = Pattern.compile("[:/?#\\[\\]@!$&'()*+,;=]");
                Matcher matcher = illegal.matcher(datasetOrUserValue);
                if (matcher.find()) {
                    log.error("Illegal characters found in the user name: {}. Will fail with a bad request.", datasetOrUser);
                    response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Illegal characters found in the user name: '" + datasetOrUser + "'. The following characters are not allowed: : / ? # [ ] @ ! $ & ' ( ) * + , ; =");
                    return false;
                }
                parameters.user = datasetOrUserValue;
                parameters.dataset = null;
                return true;
            default:
                log.error("Invalid required parameter: {}. Will fail with a bad request", datasetOrUser);
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
            response.sendError(HttpServletResponse.SC_NOT_FOUND, errorMessage);
        } else {
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType(IO_MIME_TYPE);
            try (PrintWriter pw = response.getWriter()) {
                output.writeJSONString(pw);
            }
        }
    }

    private JSONObject getDatasetStatistics(String dataset) throws IOException {
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
        Object datasetStats = allStats.get(dataset);
        if (datasetStats == null) return new JSONObject();
            // Get dataset description and uploader user name
        else {
            JSONObject stats = (JSONObject) datasetStats;
            String query = DATASET_INFO_QUERY.replace(DATASET_PLACE_HOLDER, dataset);
            TupleQueryResult result = runSparqlQuery(query);
            try {
                while (result.hasNext()) {
                    Value descriptionOrUploader = result.next().getValue("description_or_uploader");
                    if (descriptionOrUploader instanceof org.openrdf.model.URI)
                        stats.put("uploader", descriptionOrUploader.stringValue());
                    else stats.put("description", descriptionOrUploader.stringValue());
                }
                return stats;
            } catch (QueryEvaluationException qee) {
                log.error("Failed evaluating the dataset statistics query: '" + query + "' The stack trace follows.", qee);
                return null;
            }
        }
    }

    private JSONObject getUserStatistics(String user) {
        JSONObject stats = new JSONObject();
        String query = USER_QUERY.replace(USER_PLACE_HOLDER, user);
        TupleQueryResult result = runSparqlQuery(query);
        try {
            while (result.hasNext()) {
                int activities = Integer.parseInt(result.next().getValue("activities").stringValue());
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
