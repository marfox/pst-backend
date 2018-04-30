package org.wikidata.query.rdf.primarysources.statistics;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.primarysources.common.ApiParameters;
import org.wikidata.query.rdf.primarysources.common.EntitiesCache;

/**
 * Shared logic for request and response processing of {@link PropertiesServlet} and {@link ValuesServlet}.
 *
 * @author Marco Fossati - <a href="https://meta.wikimedia.org/wiki/User:Hjfocs">User:Hjfocs</a>
 * @since 0.2.5
 * Created on Feb 28, 2018.
 */
class SessionHandler {

    private static final Logger log = LoggerFactory.getLogger(SessionHandler.class);

    private String dataset;

    boolean processRequest(HttpServletRequest request, HttpServletResponse response) throws IOException {
        Map<String, String[]> params = request.getParameterMap();
        if (params.size() > 1) {
            log.warn("More than one parameter given, will fail with a bad request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Only one optional parameter allowed: '" + ApiParameters.DATASET_PARAMETER + "'");
            return false;
        }
        if (params.size() == 1 && !params.keySet().contains(ApiParameters.DATASET_PARAMETER)) {
            log.warn("Invalid optional parameter given. Will fail with a bad request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid optional parameter given. Only '" + ApiParameters.DATASET_PARAMETER + "' is " +
                "allowed");
            return false;
        }
        String datasetParameter = request.getParameter(ApiParameters.DATASET_PARAMETER);
        if (datasetParameter == null || datasetParameter.isEmpty()) {
            dataset = "all";
        } else {
            try {
                new URI(datasetParameter);
                dataset = datasetParameter;
            } catch (URISyntaxException use) {
                log.warn("Invalid dataset URI: {}. Parse error at index {}. Will fail with a bad request", use.getInput(), use.getIndex());
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid dataset URI: <" + use.getInput() + ">. " +
                    "Parse error at index " + use.getIndex() + ".");
                return false;
            }
        }
        log.debug("Required parameters stored: {}", dataset);
        return true;
    }

    void sendResponse(HttpServletResponse response, JSONObject entities, String entityType) throws IOException {
        if (entities == null) {
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Something went wrong when getting the list of " + entityType + ".");
        } else if (entities.isEmpty()) {
            log.warn("No {} available. Will fail with a not found", entityType);
            response.sendError(HttpServletResponse.SC_NOT_FOUND, "No " + entityType + " available.");
        } else {
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType(ApiParameters.DEFAULT_IO_CONTENT_TYPE);
            try (PrintWriter pw = response.getWriter()) {
                entities.writeJSONString(pw);
            }
        }
    }

    JSONObject getEntities(String entityType) {
        Path cache;
        switch (entityType) {
        case "subjects":
            cache = EntitiesCache.SUBJECTS_CACHE_FILE;
            break;
        case "properties":
            cache = EntitiesCache.PROPERTIES_CACHE_FILE;
            break;
        case "values":
            cache = EntitiesCache.VALUES_CACHE_FILE;
            break;
        default:
            log.error("Unexpected entity type: {}. The cache for those entities cannot be retrieved", entityType);
            return null;
        }
        JSONParser parser = new JSONParser();
        Object parsed;
        try (BufferedReader reader = Files.newBufferedReader(cache)) {
            try {
                parsed = parser.parse(reader);
            } catch (ParseException pe) {
                log.error("The {} cache is malformed JSON. Parse error at index {}. Please check {}", entityType, pe.getPosition(), cache);
                return null;
            }
        } catch (IOException ioe) {
            log.error("Failed to load the {} cache file: {}. Reason: {}", entityType, cache, ioe.getClass().getSimpleName());
            return null;
        }
        JSONObject allEntities = (JSONObject) parsed;
        if (dataset.equals("all")) {
            log.debug("All {} from cache file '{}:' {}", entityType, cache, allEntities);
            return allEntities;
        } else {
            if (allEntities.containsKey(dataset)) {
                JSONObject datasetEntities = new JSONObject();
                JSONArray entities = (JSONArray) allEntities.get(dataset);
                datasetEntities.put(dataset, entities);
                log.debug("<{}> {} from cache file '{}:' {}", dataset, entityType, cache, datasetEntities);
                return datasetEntities;
            } else return new JSONObject(); // Stands for no available entities
        }
    }
}
