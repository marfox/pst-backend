package org.wikidata.query.rdf.primarysources.curation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.openrdf.model.Value;
import org.openrdf.query.TupleQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.primarysources.common.ApiParameters;
import org.wikidata.query.rdf.primarysources.common.EntitiesCache;
import org.wikidata.query.rdf.primarysources.common.Utils;

/**
 * Get statements to be curated about a random subject item.
 * <p>
 * The output statements are serialized in <i>QuickStatements</i>.
 * See the <a href="https://www.wikidata.org/wiki/Help:QuickStatements#Command_sequence_syntax">syntax specifications</a>.
 * <p>
 * Support for <i>Wikidata JSON</i> output can be implemented as a method similar to {@link Utils#formatSuggestions(TupleQueryResult, String, String)}
 * in the private method {@code sendResponse} of this servlet.
 * The conversion logic between Wikidata RDF and Wikidata JSON is already available, see for instance {@link Utils#rdfValueToWikidataJson(Value)}.
 * <p>
 * This service is part of the Wikidata primary sources tool <i>Curation API</i>:
 * see <a href="https://upload.wikimedia.org/wikipedia/commons/a/a7/Wikidata_primary_sources_tool_architecture_v2.svg">this picture</a>
 * for an overview of the tool architecture.
 *
 * @author Marco Fossati - <a href="https://meta.wikimedia.org/wiki/User:Hjfocs">User:Hjfocs</a>
 * @since 0.2.5 - created on Dec 05, 2017.
 */
public class RandomServlet extends HttpServlet {

    private static final Logger log = LoggerFactory.getLogger(RandomServlet.class);

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        RequestParameters parameters = new RequestParameters();
        boolean ok = processRequest(request, response, parameters);
        if (!ok) return;
        Set<String> items = readCachedSubjectSet(parameters.dataset);
        if (items == null) {
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Something went wrong when retrieving the list of subject items.");
            return;
        }
        log.info("Loaded subject items from cache");
        parameters.qId = pickRandomItem(new ArrayList<>(items));
        log.debug("Required parameters stored as fields in private class: {}", parameters);
        TupleQueryResult suggestions = Utils.getSuggestions(parameters.dataset, parameters.qId);
        sendResponse(response, suggestions, parameters);
        log.info("GET /random successful");
    }

    private Set<String> readCachedSubjectSet(String dataset) {
        Set<String> subjectSet = new HashSet<>();
        JSONParser parser = new JSONParser();
        Object parsed;
        try (BufferedReader reader = Files.newBufferedReader(EntitiesCache.SUBJECTS_CACHE_FILE)) {
            try {
                parsed = parser.parse(reader);
            } catch (ParseException pe) {
                log.error("Malformed JSON subject list. Parse error at index {}. Please check {}", pe.getPosition(), EntitiesCache.SUBJECTS_CACHE_FILE);
                return null;
            }
        } catch (IOException ioe) {
            log.error("Failed to load the subjects cache file: {}. Reason: {}", EntitiesCache.SUBJECTS_CACHE_FILE, ioe.getClass().getSimpleName());
            return null;
        }
        JSONObject subjects = (JSONObject) parsed;
        if (dataset.equals("all"))
            for (String ds : (Set<String>) subjects.keySet()) subjectSet.addAll((JSONArray) subjects.get(ds));
        else subjectSet.addAll((JSONArray) subjects.get(dataset));
        log.debug("Subject items from cache file '{}:' {}", EntitiesCache.SUBJECTS_CACHE_FILE, subjectSet);
        return subjectSet;
    }

    private String pickRandomItem(List<String> items) {
        Random random = new Random();
        int randomIndex = random.nextInt(items.size());
        log.debug("Will pick subject number {}", randomIndex);
        return items.get(randomIndex);
    }

    private void sendResponse(HttpServletResponse response, TupleQueryResult suggestions, RequestParameters parameters) throws IOException {
        JSONArray jsonSuggestions = Utils.formatSuggestions(suggestions, parameters.dataset, parameters.qId);
        if (jsonSuggestions == null) {
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Something went wrong when retrieving suggestions.");
        } else if (jsonSuggestions.isEmpty()) {
            log.warn("No suggestions available for item {}. Will fail with a not found", parameters.qId);
            response.sendError(HttpServletResponse.SC_NOT_FOUND, "No suggestions available for item " + parameters.qId + " .");
        } else {
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType(ApiParameters.DEFAULT_IO_CONTENT_TYPE);
            try (PrintWriter pw = response.getWriter()) {
                jsonSuggestions.writeJSONString(pw);
            }
        }
    }

    private boolean processRequest(HttpServletRequest request, HttpServletResponse response, RequestParameters parameters) throws IOException {
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

    private class RequestParameters {
        private String dataset;
        private String qId;

        @Override
        public String toString() {
            return String.format(Locale.ENGLISH, "dataset = %s; QID = %s", dataset, qId);
        }
    }
}
