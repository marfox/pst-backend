package org.wikidata.query.rdf.primarysources.curation;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.uri.Provenance;
import org.wikidata.query.rdf.common.uri.WikibaseUris;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.wikidata.query.rdf.primarysources.common.EntitiesCache.SUBJECTS_CACHE_FILE;
import static org.wikidata.query.rdf.primarysources.curation.SuggestServlet.*;

/**
 * @author Marco Fossati - User:Hjfocs
 * @since 0.2.5
 * Created on Dec 05, 2017.
 */
public class RandomServlet extends HttpServlet {
    private static final Logger log = LoggerFactory.getLogger(RandomServlet.class);

    private class RequestParameters {
        private String dataset;
        private String qId;
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        RequestParameters parameters = new RequestParameters();
        boolean ok = processRequest(request, response, parameters);
        if (!ok) return;
        Set<String> items =  readCachedSubjectSet(parameters.dataset);
        if (items == null) {
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Something went wrong when retrieving the list of subject items.");
            return;
        }
        parameters.qId = pickRandomItem(new ArrayList<>(items));
        TupleQueryResult suggestions = getSuggestions(parameters);
        sendResponse(response, suggestions, parameters);
    }

    private Set<String> readCachedSubjectSet(String dataset) throws IOException {
        Set<String> subjectSet = new HashSet<>();
        JSONParser parser = new JSONParser();
        Object parsed;
        try (BufferedReader reader = Files.newBufferedReader(SUBJECTS_CACHE_FILE)) {
            try {
                parsed = parser.parse(reader);
            } catch (ParseException pe) {
                log.error("Malformed JSON subject list. Parse error at index {}. Please check {}", pe.getPosition(), SUBJECTS_CACHE_FILE);
                return null;
            }
        }
        JSONObject subjects = (JSONObject) parsed;
        if (dataset.equals("all"))
            for (String ds : (Set<String>) subjects.keySet()) subjectSet.addAll((JSONArray) subjects.get(ds));
        else subjectSet.addAll((JSONArray) subjects.get(dataset));
        return subjectSet;
    }

    private String pickRandomItem(List<String> items) {
        Random random = new Random();
        int randomIndex = random.nextInt(items.size());
        return items.get(randomIndex);
    }

    private TupleQueryResult getSuggestions(RequestParameters parameters) throws IOException {
        String query = parameters.dataset.equals("all") ? ALL_DATASETS_QUERY.replace(QID_PLACE_HOLDER, parameters.qId) : ONE_DATASET_QUERY.replace(QID_PLACE_HOLDER, parameters.qId).replace(DATASET_PLACE_HOLDER, parameters.dataset);
        return runSparqlQuery(query);
    }

    private JSONArray formatSuggestions(TupleQueryResult suggestions, RequestParameters parameters) {
        JSONArray jsonSuggestions = new JSONArray();
        String qualifierPrefix = WIKIBASE_URIS.property(WikibaseUris.PropertyType.QUALIFIER);
        String referencePrefix = Provenance.WAS_DERIVED_FROM;
        Map<String, StringBuilder> quickStatements = new HashMap<>();
        try {
            while (suggestions.hasNext()) {
                BindingSet suggestion = suggestions.next();
                Value datasetValue = suggestion.getValue("dataset");
                String currentDataset = datasetValue == null ? parameters.dataset : datasetValue.stringValue();
                String mainProperty = suggestion.getValue("property").stringValue().substring(WIKIBASE_URIS.property(WikibaseUris.PropertyType.CLAIM).length());
                String statementUuid = suggestion.getValue("statement_node").stringValue().substring(WIKIBASE_URIS.statement().length());
                String statementProperty = suggestion.getValue("statement_property").stringValue();
                Value statementValue = suggestion.getValue("statement_value");
                String qsKey = statementUuid + "|" + currentDataset;
                // Statement
                if (statementProperty.startsWith(WIKIBASE_URIS.property(WikibaseUris.PropertyType.STATEMENT))) {
                    StringBuilder quickStatement = quickStatements.getOrDefault(qsKey, new StringBuilder());
                    quickStatements.put(
                            qsKey,
                            quickStatement.insert(0, parameters.qId + "\t" + mainProperty + "\t" + rdfValueToQuickStatement(statementValue)));
                }
                // Qualifier
                else if (statementProperty.startsWith(qualifierPrefix)) {
                    StringBuilder quickStatement = quickStatements.getOrDefault(qsKey, new StringBuilder());
                    quickStatements.put(
                            qsKey,
                            quickStatement
                                    .append("\t")
                                    .append(statementProperty.substring(qualifierPrefix.length()))
                                    .append("\t")
                                    .append(rdfValueToQuickStatement(statementValue)));
                }
                // Reference
                else if (statementProperty.equals(referencePrefix)) {
                    String referenceProperty = suggestion.getValue("reference_property").stringValue();
                    Value referenceValue = suggestion.getValue("reference_value");
                    StringBuilder quickStatement = quickStatements.getOrDefault(qsKey, new StringBuilder());
                    quickStatements.put(
                            qsKey,
                            quickStatement
                                    .append("\t")
                                    .append(referenceProperty.substring(WIKIBASE_URIS.property(WikibaseUris.PropertyType.REFERENCE).length()).replace("P", "S"))
                                    .append("\t")
                                    .append(rdfValueToQuickStatement(referenceValue)));
                }
            }
        } catch (QueryEvaluationException qee) {
            log.error("Failed evaluating the suggestion query: {}", qee.getMessage());
            return null;
        }
        for (String key : quickStatements.keySet()) {
            String dataset = key.split("\\|")[1];
            String qs = quickStatements.get(key).toString();
            // The front end always thinks there is only 1 reference, so mint 1 QuickStatement per reference
            Pattern pattern = Pattern.compile("\tS\\d+\t[^\t]+");
            Matcher matcher = pattern.matcher(qs);
            List<String> references = new ArrayList<>();
            while (matcher.find()) {
                String reference = matcher.group();
                references.add(reference);
                qs = qs.replace(reference, "");
            }
            for (String ref : references) {
                String statement = qs + ref;
                JSONObject jsonSuggestion = new JSONObject();
                jsonSuggestion.put("dataset", dataset);
                jsonSuggestion.put("format", "QuickStatement");
                jsonSuggestion.put("state", "new");
                jsonSuggestion.put("statement", statement);
                jsonSuggestions.add(jsonSuggestion);
            }
        }
        return jsonSuggestions;
    }

    private void sendResponse(HttpServletResponse response, TupleQueryResult suggestions, RequestParameters parameters) throws IOException {
        response.setHeader("Access-Control-Allow-Origin", "*");
        JSONArray jsonSuggestions = formatSuggestions(suggestions, parameters);
        if (jsonSuggestions == null) {
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Something went wrong when retrieving suggestions.");
        } else if (jsonSuggestions.isEmpty()) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND, "No suggestions available for item " + parameters.qId + " .");
        } else {
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType(IO_MIME_TYPE);
            try (PrintWriter pw = response.getWriter()) {
                jsonSuggestions.writeJSONString(pw);
            }
        }
    }

    private boolean processRequest(HttpServletRequest request, HttpServletResponse response, RequestParameters parameters) throws IOException {
        String datasetParameter = request.getParameter(DATASET_PARAMETER);
        if (datasetParameter == null || datasetParameter.isEmpty()) {
            parameters.dataset = "all";
        } else {
            try {
                new URI(datasetParameter);
                parameters.dataset = datasetParameter;
            } catch (URISyntaxException use) {
                log.error("Invalid dataset URI: {}. Parse error at index {}. Will fail with a bad request", use.getInput(), use.getIndex());
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid dataset URI: <" + use.getInput() + ">. " +
                        "Parse error at index " + use.getIndex() + ".");
                return false;
            }
        }
        return true;
    }
}
