package org.wikidata.query.rdf.primarysources.statistics;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.wikidata.query.rdf.primarysources.common.EntitiesCache.*;
import static org.wikidata.query.rdf.primarysources.curation.SuggestServlet.DATASET_PARAMETER;
import static org.wikidata.query.rdf.primarysources.curation.SuggestServlet.IO_MIME_TYPE;

/**
 * @author Marco Fossati - User:Hjfocs
 * @since 0.2.5
 * Created on Feb 28, 2018.
 */
public class SessionHandler {

    private String dataset;

    boolean processRequest(HttpServletRequest request, HttpServletResponse response, Logger log) throws IOException {
        Map<String, String[]> params = request.getParameterMap();
        if (params.size() > 1) {
            log.error("More than one parameter given, will fail with a bad request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Only one optional parameter allowed: '" + DATASET_PARAMETER + "'");
            return false;
        }
        if (params.size() == 1 && !params.keySet().contains(DATASET_PARAMETER)) {
            log.error("Invalid optional parameter given. Will fail with a bad request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid optional parameter given. Only '" + DATASET_PARAMETER + "' is allowed");
            return false;
        }
        String datasetParameter = request.getParameter(DATASET_PARAMETER);
        if (datasetParameter == null || datasetParameter.isEmpty()) {
            dataset = "all";
        } else {
            try {
                new URI(datasetParameter);
                dataset = datasetParameter;
            } catch (URISyntaxException use) {
                log.error("Invalid dataset URI: {}. Parse error at index {}. Will fail with a bad request", use.getInput(), use.getIndex());
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid dataset URI: <" + use.getInput() + ">. " +
                        "Parse error at index " + use.getIndex() + ".");
                return false;
            }
        }
        return true;
    }

    void sendResponse(HttpServletResponse response, JSONObject entities, String entityType) throws IOException {
        response.setHeader("Access-Control-Allow-Origin", "*");
        if (entities == null) {
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Something went wrong when getting the list of " + entityType + ".");
        } else if (entities.isEmpty()) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND, "Sorry, no " + entityType + " available.");
        } else {
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType(IO_MIME_TYPE);
            try (PrintWriter pw = response.getWriter()) {
                entities.writeJSONString(pw);
            }
        }
    }

    JSONObject getEntities(String entityType, Logger log) throws IOException {
        Path cache;
        switch (entityType) {
            case "subjects":
                cache = SUBJECTS_CACHE_FILE;
                break;
            case "properties":
                cache = PROPERTIES_CACHE_FILE;
                break;
            case "values":
                cache = VALUES_CACHE_FILE;
                break;
            default:
                log.error("Unexpected entity type '{}'. The cache for those entities cannot be retrieved", entityType);
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
        }
        JSONObject allEntities = (JSONObject) parsed;
        if (dataset.equals("all")) return allEntities;
        else {
            if (allEntities.containsKey(dataset)) {
                JSONObject datasetEntities = new JSONObject();
                JSONArray entities = (JSONArray) allEntities.get(dataset);
                datasetEntities.put(dataset, entities);
                return datasetEntities;
            } else return new JSONObject(); // Stands for no available entities
        }
    }
}
